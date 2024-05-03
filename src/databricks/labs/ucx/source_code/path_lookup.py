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
        self._cwd = cwd
        self._sys_paths = sys_paths

    def change_directory(self, new_working_directory: Path) -> PathLookup:
        return PathLookup(new_working_directory, self._sys_paths)

    def resolve(self, path: Path) -> Path | None:
        if path.is_absolute():
            return path
        for parent in self.paths:
            absolute_path = parent / path
            if absolute_path.exists():
                return absolute_path
        return None

    def push_path(self, path: Path):
        self._sys_paths.insert(0, path)

    def insert_path(self, index: int, path: Path):
        self._sys_paths.insert(index, path)

    def remove_path(self, index: int):
        del self._sys_paths[index]

    def pop_path(self) -> Path:
        result = self._sys_paths[0]
        del self._sys_paths[0]
        return result

    @property
    def paths(self) -> list[Path]:
        return [self.cwd] + self._sys_paths

    @property
    def cwd(self):
        return self._cwd

    def __repr__(self):
        return f"PathLookup(cwd={self._cwd}, sys_paths={self._sys_paths})"
