from __future__ import annotations

import os
from collections import OrderedDict
from collections.abc import Generator
from io import StringIO, BytesIO

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo
from databricks.labs.blueprint.paths import WorkspacePath


class _CachedIO:

    def __init__(self, content):
        self._content = content
        self._index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def read(self, *args, **_kwargs):
        count = -1 if len(args) < 1 or args[0] < 1 else args[0]
        if count == -1:
            return self._content
        start = self._index
        end = self._index + count
        if start >= len(self._content):
            return None
        self._index = self._index + count
        return self._content[start:end]

    def __iter__(self):
        if isinstance(self._content, str):
            yield from StringIO(self._content)
            return
        yield from self._as_string_io().__iter__()

    def with_mode(self, mode: str):
        if 'b' in mode:
            return self._as_bytes_io()
        return self._as_string_io()

    def _as_bytes_io(self):
        if isinstance(self._content, bytes):
            return self
        return BytesIO(self._content.encode("utf-8-sig"))

    def _as_string_io(self):
        if isinstance(self._content, str):
            return self
        return StringIO(self._content.decode("utf-8"))


# lru_cache won't let us invalidate cache entries
# so we provide our own custom lru_cache
class _PathLruCache:

    def __init__(self, max_entries: int):
        self._datas: OrderedDict[str, bytes | str] = OrderedDict()
        self._max_entries = max_entries

    def open(self, cached_path: _CachedPath, mode, buffering, encoding, errors, newline):
        path = str(cached_path)
        if path in self._datas:
            self._datas.move_to_end(path)
            return _CachedIO(self._datas[path]).with_mode(mode)
        io_obj = WorkspacePath.open(cached_path, mode, buffering, encoding, errors, newline)
        # can't read twice from an IO so need to cache data rather than the io object
        data = io_obj.read()
        self._datas[path] = data
        result = _CachedIO(data).with_mode(mode)
        if len(self._datas) > self._max_entries:
            self._datas.popitem(last=False)
        return result

    def clear(self):
        self._datas.clear()

    def remove(self, path: str):
        if path in self._datas:
            self._datas.pop(path)


class _CachedPath(WorkspacePath):
    def __init__(self, cache: _PathLruCache, ws: WorkspaceClient, *args: str | bytes | os.PathLike):
        super().__init__(ws, *args)
        self._cache = cache

    def with_object_info(self, object_info: ObjectInfo):
        self._cached_object_info = object_info
        return self

    def with_segments(self, *path_segments: bytes | str | os.PathLike) -> _CachedPath:
        return type(self)(self._cache, self._ws, *path_segments)

    def iterdir(self) -> Generator[_CachedPath, None, None]:
        for object_info in self._ws.workspace.list(self.as_posix()):
            path = object_info.path
            if path is None:
                msg = f"Cannot initialise without object path: {object_info}"
                raise ValueError(msg)
            child = _CachedPath(self._cache, self._ws, path)
            yield child.with_object_info(object_info)

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        # only cache reads
        if 'r' in mode:
            return self._cache.open(self, mode, buffering, encoding, errors, newline)
        self._cache.remove(str(self))
        return super().open(mode, buffering, encoding, errors, newline)

    def _cached_open(self, mode: str, buffering: int, encoding: str | None, errors: str | None, newline: str | None):
        return super().open(mode, buffering, encoding, errors, newline)

    # _rename calls unlink so no need to override it
    def unlink(self, missing_ok: bool = False) -> None:
        self._cache.remove(str(self))
        return super().unlink(missing_ok)


class WorkspaceCache:

    def __init__(self, ws: WorkspaceClient, max_entries=2048):
        self._ws = ws
        self._cache = _PathLruCache(max_entries)

    def get_path(self, path: str):
        return _CachedPath(self._cache, self._ws, path)
