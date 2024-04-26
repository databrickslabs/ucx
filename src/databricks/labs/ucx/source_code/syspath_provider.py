import sys
from collections.abc import Iterable
from pathlib import Path


class SysPathProvider:

    @classmethod
    def from_pathlike_string(cls, syspath: str):
        paths = syspath.split(':')
        return SysPathProvider([Path(path) for path in paths])

    @classmethod
    def from_sys_path(cls):
        return SysPathProvider([Path(path) for path in sys.path])

    def __init__(self, paths: list[Path]):
        self._paths = paths

    def push(self, path: Path):
        self._paths.insert(0, path)

    def pop(self) -> Path:
        result = self._paths[0]
        del self._paths[0]
        return result

    @property
    def paths(self) -> Iterable[Path]:
        yield from self._paths
