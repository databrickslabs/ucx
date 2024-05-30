from __future__ import annotations

import logging
import sys
from pathlib import Path


logger = logging.getLogger(__name__)


class PathLookup:
    """
    Mimic Python's importlib Lookup.

    Sources:
        See Lookup in importlib.metadata.__init__.py
    """

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
        try:
            if path.is_absolute() and path.exists():
                # eliminate “..” components
                return path.resolve()
        except PermissionError:
            logger.warning(f"Permission denied to access {path}")
            return None
        for library_root in self.library_roots:
            resolved_path = self._resolve_library_root(library_root, path)
            if resolved_path is not None:
                return resolved_path
        return None

    def _resolve_library_root(self, library_root: Path, path: Path) -> Path | None:
        try:
            if not library_root.is_dir():
                return None
        except PermissionError:
            logger.warning(f"Permission denied to access (subfolders of) {library_root}")
            return None
        absolute_path = library_root / path
        try:
            if absolute_path.exists():
                return absolute_path.resolve()  # eliminate “..” components
        except PermissionError:
            logger.warning(f"Permission denied to access {absolute_path}")
        return self._resolve_egg_library(library_root, path)

    def _resolve_egg_library(self, library: Path, path: Path) -> Path | None:
        for child in library.iterdir():
            try:
                if not self._is_egg_folder(child):
                    continue
            except PermissionError:
                logger.warning(f"Permission denied to access {child}")
                continue
            absolute_path = child / path
            try:
                if absolute_path.exists():
                    return absolute_path.resolve()  # eliminate “..” components
            except PermissionError:
                logger.warning(f"Permission denied to access {absolute_path}")
        return None

    @staticmethod
    def _is_egg_folder(path: Path) -> bool:
        """Egg folders end with `.egg` and have a 'EGG-INFO' file."""
        return (
            path.is_dir()
            and path.suffix == ".egg"
            and any(subfolder.name.lower() == "egg-info" for subfolder in path.iterdir())
        )

    def has_path(self, path: Path):
        return next(p for p in self._sys_paths if path == p) is not None

    def prepend_path(self, path: Path):
        self._sys_paths.insert(0, path)

    def insert_path(self, index: int, path: Path):
        self._sys_paths.insert(index, path)

    def append_path(self, path: Path):
        if path in self._sys_paths:
            return
        self._sys_paths.append(path)

    def remove_path(self, index: int):
        del self._sys_paths[index]

    @property
    def library_roots(self) -> list[Path]:
        # we may return a combination of WorkspacePath and PosixPath here
        return [self._cwd] + self._sys_paths

    @property
    def cwd(self):
        return self._cwd

    def __repr__(self):
        return f"PathLookup(cwd={self._cwd}, sys_paths={self._sys_paths})"
