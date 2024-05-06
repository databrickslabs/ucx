from __future__ import annotations

import sys
from collections.abc import Iterable
from pathlib import Path


class SysPathLookup:

    @classmethod
    def from_pathlike_string(cls, cwd: Path, syspath: str):
        paths = syspath.split(':')
        return SysPathLookup(cwd, [Path(path) for path in paths])

    @classmethod
    def from_sys_path(cls, cwd: Path):
        return SysPathLookup(cwd, [Path(path) for path in sys.path])

    def __init__(self, cwd: Path, sys_paths: list[Path]):
        self._cwd = cwd
        self._sys_paths = sys_paths

    def has_path(self, path: Path):
        return next(p for p in self._sys_paths if path == p) is not None

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
        yield self._cwd
        yield from self._sys_paths

    @property
    def cwd(self):
        return self._cwd

    @cwd.setter
    def cwd(self, cwd: Path):
        self._cwd = cwd
