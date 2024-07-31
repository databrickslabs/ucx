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
                # eliminate ".." components
                return path.resolve()
        except PermissionError:
            logger.warning(f"Permission denied to access {path}")
            return None
        for library_root in self.library_roots:
            try:
                resolved_path = self._resolve_in_library_root(library_root, path)
            except PermissionError:
                logger.warning(f"Permission denied to access files or directories in {library_root}")
                continue
            if resolved_path is not None:
                return resolved_path
        return None

    def _resolve_in_library_root(self, library_root: Path, path: Path) -> Path | None:
        if not library_root.is_dir():
            return None
        absolute_path = library_root / path
        if absolute_path.exists():
            return self._standardize_path(absolute_path)
        return self._resolve_egg_in_library_root(library_root, path)

    def _resolve_egg_in_library_root(self, library: Path, path: Path) -> Path | None:
        for child in library.iterdir():
            if not self._is_egg_folder(child):
                continue
            absolute_path = child / path
            if absolute_path.exists():
                return self._standardize_path(absolute_path)
        return None

    @staticmethod
    def _standardize_path(path: Path):
        resolved = path.resolve()  # eliminate ".." components
        # on MacOS "/var/..." resolves to "/private/var/.." which breaks path equality
        index = 1 if path.parts[0] == path.anchor else 0
        if path.parts[index] == resolved.parts[index]:
            return resolved
        posix = resolved.as_posix()
        posix = posix[posix.index(path.as_posix()) :]
        return Path(posix)

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
        # we may return a combination of DBFSPath, WorkspacePath and PosixPath here
        library_roots = []
        for library_root in [self._cwd] + self._sys_paths:
            try:
                is_existing_directory = library_root.exists() and library_root.is_dir()
            except PermissionError:
                continue
            if is_existing_directory:
                library_roots.append(library_root)
        return library_roots

    @property
    def cwd(self):
        return self._cwd

    def __repr__(self):
        return f"PathLookup(cwd={self._cwd}, sys_paths={self._sys_paths})"
