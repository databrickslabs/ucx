from collections.abc import Iterable
from pathlib import Path


class SysPathProvider:

    @staticmethod
    def initialize(syspath: str):
        paths = syspath.split(':')
        return SysPathProvider([Path(path) for path in paths])

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
