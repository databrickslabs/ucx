from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


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
        if path.is_absolute() and path.exists():
            return path
        for parent in self.paths:
            absolute_path = parent / path
            try:
                if absolute_path.exists():
                    return absolute_path
            except PermissionError:
                logger.warning(f"Permission denied to access {absolute_path}")
        return None

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
    def paths(self) -> list[Path]:
        return [self.cwd] + self._sys_paths

    @property
    def cwd(self):
        return self._cwd

    def __repr__(self):
        return f"PathLookup(cwd={self._cwd}, sys_paths={self._sys_paths})"
