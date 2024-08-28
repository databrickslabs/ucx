from collections import OrderedDict
from io import StringIO, BytesIO
from threading import local

from databricks.labs.blueprint.paths import WorkspacePath

thread_local = local()
thread_local.cache = OrderedDict()


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

    _CACHE_MAX_SIZE = 2048
    _CACHE: OrderedDict[str, bytes | str] = thread_local.cache

    def __init__(self, func):
        self._func = func

    def __call__(self, *args, **kwargs):
        path = str(args[0])  # the CachedWorkspacePath instance
        cache = self._CACHE
        if path in cache:
            cache.move_to_end(path)
            return _CachedIO(cache[path]).with_mode(args[1])
        io_obj = self._func(*args, **kwargs)
        # can't read twice from an IO so need to cache content rather than the result
        data = io_obj.read()
        cache[path] = data
        result = _CachedIO(data).with_mode(args[1])
        if len(cache) > self._CACHE_MAX_SIZE:
            cache.popitem(last=False)
        return result

    @classmethod
    def clear(cls):
        cls._CACHE.clear()

    @classmethod
    def remove(cls, path: str):
        if path in cls._CACHE:
            cls._CACHE.pop(path)


def _workspace_lru_cache(func):

    cache = _PathLruCache(func)

    # we need a wrapper to receive self in args
    def wrapper(*args, **kwargs):
        nonlocal cache
        return cache(*args, **kwargs)

    return wrapper


def clear_path_lru_cache():
    _PathLruCache.clear()


class CachedPath(WorkspacePath):

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
            return self._cached_open(mode, buffering, encoding, errors, newline)
        _PathLruCache.remove(str(self))
        return super().open(mode, buffering, encoding, errors, newline)

    @_workspace_lru_cache
    def _cached_open(self, mode: str, buffering: int, encoding: str | None, errors: str | None, newline: str | None):
        return super().open(mode, buffering, encoding, errors, newline)

    # _rename calls unlink so no need to override it
    def unlink(self, missing_ok: bool = False) -> None:
        _PathLruCache.remove(str(self))
        return super().unlink(missing_ok)
