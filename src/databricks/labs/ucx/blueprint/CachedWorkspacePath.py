from collections import OrderedDict
from io import StringIO
from typing import TextIO, BinaryIO

from databricks.labs.blueprint.paths import WorkspacePath


class _CachedIO:

    def __init__(self, wrapped):
        self._type = type(wrapped)
        self._content = wrapped.read()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def read(self, *_args, **_kwargs):
        return self._content

    def __iter__(self):
        yield from self._type(self._content)


# lru_cache won't let us invalidate cache entries
# so we provide our own custom lru_cache
class WorkspaceLruCache:

    _WORKSPACE_CACHE_MAX_SIZE = 128
    _WORKSPACE_CACHE: OrderedDict[str, _CachedIO] = OrderedDict()

    def __init__(self, func):
        self._func = func

    def __call__(self, *args, **kwargs):
        path = str(args[0])  # the CachedWorkspacePath instance
        cache = self._WORKSPACE_CACHE
        if path in cache:
            cache.move_to_end(path)
            return cache[path]
        result = self._func(*args, **kwargs)
        if isinstance(result, (StringIO, TextIO, BinaryIO)):
            result = _CachedIO(result)
        cache[path] = result
        if len(cache) > self._WORKSPACE_CACHE_MAX_SIZE:
            cache.popitem(last=False)
        return result

    @classmethod
    def clear(cls):
        cls._WORKSPACE_CACHE.clear()

    @classmethod
    def remove(cls, path: str):
        if path in cls._WORKSPACE_CACHE:
            cls._WORKSPACE_CACHE.pop(path)


def workspace_lru_cache(func):

    cache = WorkspaceLruCache(func)

    # we need a wrapper to receive self in args
    def wrapper(*args, **kwargs):
        nonlocal cache
        return cache(*args, **kwargs)

    return wrapper


class CachedWorkspacePath(WorkspacePath):

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ):
        if 'r' in mode:
            return self._cached_open(mode, buffering, encoding, errors, newline)
        WorkspaceLruCache.remove(str(self))
        return super().open(mode, buffering, encoding, errors, newline)

    @workspace_lru_cache
    def _cached_open(self, mode: str, buffering: int, encoding: str | None, errors: str | None, newline: str | None):
        return super().open(mode, buffering, encoding, errors, newline)

    # _rename calls unlink so no need to override it
    def unlink(self, missing_ok: bool = False) -> None:
        WorkspaceLruCache.remove(str(self))
        return super().unlink(missing_ok)
