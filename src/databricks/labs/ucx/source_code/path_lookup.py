from __future__ import annotations

import sys
from collections.abc import Iterable
from pathlib import Path


class PathLookup:

    @classmethod
    def from_pathlike_string(cls, cwd: Path, syspath: str):
        paths = syspath.split(':')
        return PathLookup(cwd, [Path(path) for path in paths])

    @classmethod
    def from_sys_path(cls, cwd: Path):
        return PathLookup(cwd, [Path(path) for path in sys.path])

    def __init__(self, cwd: Path, sys_paths: list[Path]):
        self._sys_paths = sys_paths
        self._cwds = [cwd]

    def prepend_path(self, path: Path):
        self._sys_paths.insert(0, path)

    def insert_path(self, index: int, path: Path):
        self._sys_paths.insert(index, path)

    def append_path(self, path: Path):
        self._sys_paths.append(path)

    def remove_path(self, index: int):
        del self._sys_paths[index]

    @property
    def paths(self) -> Iterable[Path]:
        yield self.cwd
        yield from self._sys_paths

    def push_cwd(self, path: Path):
        self._cwds.append(path)

    def pop_cwd(self):
        result = self._cwds[0]
        del self._cwds[0]
        return result

    @property
    def cwd(self):
        # the below might fail but that's better than returning an incorrect cwd
        assert len(self._cwds) > 0
        return self._cwds[-1]
