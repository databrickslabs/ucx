from __future__ import annotations

import codecs
import io
import os
from collections import OrderedDict
from collections.abc import Generator
from io import BytesIO
from pathlib import PurePosixPath
from typing import IO, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo
from databricks.labs.blueprint.paths import WorkspacePath


# lru_cache won't let us invalidate cache entries
# so we provide our own custom lru_cache
class _PathLruCache:

    _datas: OrderedDict[PurePosixPath, bytes]
    """Cached binary data of files, keyed by resolved workspace path, ordered from oldest to newest."""

    _max_entries: int
    """The maximum number of entries to hold in the cache."""

    def __init__(self, max_entries: int) -> None:
        # Ordered from oldest to newest.
        self._datas = OrderedDict()
        self._max_entries = max_entries

    @classmethod
    def _normalize(cls, path: _CachedPath) -> PurePosixPath:
        # Note: must not return the same instance that was passed in, to avoid circular references (and memory leaks).
        return PurePosixPath(*path.resolve().parts)

    def load(self, cached_path: _CachedPath) -> bytes:
        normalized_path = self._normalize(cached_path)

        data = self._datas.get(normalized_path, None)
        if data is not None:
            self._datas.move_to_end(normalized_path)
            return data

        # Need to bypass the _CachedPath.open() override to actually open and retrieve the file content.
        with WorkspacePath.open(cached_path, mode="rb") as workspace_file:
            data = workspace_file.read()
        if self._max_entries <= len(self._datas):
            self._datas.popitem(last=False)
        self._datas[normalized_path] = data
        return data

    def clear(self) -> None:
        self._datas.clear()

    def remove(self, path: _CachedPath) -> None:
        del self._datas[self._normalize(path)]


class _CachedPath(WorkspacePath):
    def __init__(self, cache: _PathLruCache, ws: WorkspaceClient, *args: str | bytes | os.PathLike) -> None:
        super().__init__(ws, *args)
        self._cache = cache

    @classmethod
    def _from_object_info_with_cache(
        cls,
        cache: _PathLruCache,
        ws: WorkspaceClient,
        object_info: ObjectInfo,
    ) -> _CachedPath:
        assert object_info.path
        path = cls(cache, ws, object_info.path)
        path._cached_object_info = object_info
        return path

    def with_segments(self: _CachedPathT, *path_segments: bytes | str | os.PathLike) -> _CachedPathT:
        return type(self)(self._cache, self._ws, *path_segments)

    def iterdir(self) -> Generator[_CachedPath, None, None]:
        # Variant of the superclass implementation that preserves the cache, as well as the client.
        for object_info in self._ws.workspace.list(self.as_posix()):
            yield self._from_object_info_with_cache(self._cache, self._ws, object_info)

    @classmethod
    def _detect_encoding_bom(cls, data: io.BytesIO) -> str | None:
        # Peek at the first (up to) 4 bytes.
        maybe_bom = data.read(4)
        data.seek(-len(maybe_bom), os.SEEK_CUR)
        if maybe_bom.startswith(codecs.BOM_UTF32_LE) or maybe_bom.startswith(codecs.BOM_UTF32_BE):
            return "utf-32"
        if maybe_bom.startswith(codecs.BOM_UTF16_LE) or maybe_bom.startswith(codecs.BOM_UTF16_BE):
            return "utf-16"
        if maybe_bom.startswith(codecs.BOM_UTF8):
            return "utf-8"
        return None

    def open(  # type: ignore[override]
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO:
        # We only cache reads; if a write happens we use the default implementation (and evict any cache entry).
        if 'w' in mode:
            self._cache.remove(self)
            return super().open(mode, buffering, encoding, errors, newline)

        binary_data = self._cache.load(self)
        binary_io = BytesIO(binary_data)
        if 'b' in mode:
            return binary_io

        use_encoding = self._detect_encoding_bom(binary_io) if encoding is None else encoding
        return io.TextIOWrapper(binary_io, use_encoding, errors, newline)

    # _rename calls unlink so no need to override it
    def unlink(self, missing_ok: bool = False) -> None:
        self._cache.remove(self)
        return super().unlink(missing_ok)


_CachedPathT = TypeVar("_CachedPathT", bound=_CachedPath)


class WorkspaceCache:

    class InvalidWorkspacePath(ValueError):
        pass

    def __init__(self, ws: WorkspaceClient, max_entries: int = 2048) -> None:
        self._ws = ws
        self._cache = _PathLruCache(max_entries)

    def get_workspace_path(self, path: str) -> WorkspacePath:
        """Obtain a `WorkspacePath` instance for a path that refers to a workspace file or notebook.

        The instance returned participates in this content cache: the first time the path is opened the content will
        be immediately retrieved (prior to reading) and cached.

        Args:
            path: a valid workspace path (must be absolute)
        Raises:
            WorkspaceCache.InvalidWorkspacePath: this is raised immediately if the supplied path is not a syntactically
                valid workspace path. (This is not raised if the path is syntactically valid but does not exist.)
        """
        if not path.startswith("/"):
            msg = f"Invalid workspace path; must be absolute and start with a slash ('/'): {path}"
            raise WorkspaceCache.InvalidWorkspacePath(msg)
        return _CachedPath(self._cache, self._ws, path)
