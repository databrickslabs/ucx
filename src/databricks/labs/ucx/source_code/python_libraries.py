from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

# mypy can not analyze setuptools
from setuptools import setup  # type: ignore

from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.source_code.graph import (
    LibraryResolver,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.known import Whitelist

logger = logging.getLogger(__name__)


logger = logging.getLogger(__name__)


class PipResolver(LibraryResolver):
    # TODO: https://github.com/databrickslabs/ucx/issues/1643
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(self, whitelist: Whitelist, runner: Callable[[str], tuple[int, str, str]] = run_command) -> None:
        self._whitelist = whitelist
        self._runner = runner

    def resolve_library(self, path_lookup: PathLookup, library: Path) -> MaybeDependency:
        library_path = self._locate_library(path_lookup, library)
        if library_path is None or library_path.suffix == ".egg":  # not installed yet
            return self._install_library(path_lookup, library)
        dist_info_path = self._locate_dist_info(library_path, library)
        if dist_info_path is None:  # old package style
            problem = DependencyProblem('no-dist-info', f"No dist-info found for {library.name}")
            return MaybeDependency(None, [problem])
        return self._create_dependency(library, dist_info_path)

    def _locate_library(self, path_lookup: PathLookup, library: Path) -> Path | None:
        # start the quick way
        full_path = path_lookup.resolve(library)
        if full_path is not None:
            return full_path
        # maybe the name needs tweaking
        if "-" in library.name:
            name = library.name.replace("-", "_")
            return self._locate_library(path_lookup, Path(name))
        return None

    def _locate_dist_info(self, library_path: Path, library: Path) -> Path | None:
        if not library_path.parent.exists():
            return None
        dist_info_dir = None
        for package in library_path.parent.iterdir():
            if package.name.startswith(library.name) and package.name.endswith(".dist-info"):
                dist_info_dir = package
                break  # TODO: Matching multiple .dist-info's, https://github.com/databrickslabs/ucx/issues/1751
        if dist_info_dir is not None:
            return dist_info_dir
        # maybe the name needs tweaking
        if "-" in library.name:
            name = library.name.replace("-", "_")
            return self._locate_dist_info(library_path, Path(name))
        return None

    def _install_library(self, path_lookup: PathLookup, library: Path) -> MaybeDependency:
        """Pip install library and augment path look-up to resolve the library at import"""
        # invoke pip install via subprocess to install this library into self._lib_install_folder
        venv = self._temporary_virtual_environment(path_lookup).as_posix()
        existing_packages = os.listdir(venv)
        if library.suffix == ".egg":
            easy_install_arguments = ["easy_install", "-v", "--always-unzip", "--install-dir", venv, library.as_posix()]
            try:
                setup(script_args=easy_install_arguments)
            except SystemExit as e:
                problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")
                return MaybeDependency(None, [problem])
        else:
            try:
                subprocess.run(["pip", "install", library.name, "-t", venv], check=True)
            except CalledProcessError as e:
                problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")
                return MaybeDependency(None, [problem])
        added_packages = set(os.listdir(venv)).difference(existing_packages)
        dist_info_dirs = list(filter(lambda package: package.endswith(".dist-info"), added_packages))
        dist_info_dir = next((dir for dir in dist_info_dirs if dir.startswith(library.name)), None)
        if dist_info_dir is None and "-" in library.name:
            name = library.name.replace("-", "_")
            dist_info_dir = next((dir for dir in dist_info_dirs if dir.startswith(name)), None)
        if dist_info_dir is None:
            # TODO handle other distribution types
            problem = DependencyProblem('no-dist-info', f"No dist-info found for {library.name}")
            return MaybeDependency(None, [problem])
        dist_info_path = path_lookup.resolve(Path(dist_info_dir))
        assert dist_info_path is not None
        return self._create_dependency(library, dist_info_path)

    def _create_dependency(self, library: Path, dist_info_path: Path):
        package = DistInfo(dist_info_path)
        container = DistInfoContainer(self._file_loader, self._white_list, package)
        dependency = Dependency(WrappingLoader(container), library)
        return MaybeDependency(dependency, [])

    def _temporary_virtual_environment(self, path_lookup: PathLookup) -> Path:
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        lib_install_folder = tempfile.mkdtemp(prefix='ucx-')
        return Path(lib_install_folder)

    def _install_library(self, path_lookup: PathLookup, library: Path) -> list[DependencyProblem]:
        """Pip install library and augment path look-up to resolve the library at import"""
        venv = self._temporary_virtual_environment
        path_lookup.append_path(venv)
        # resolve relative pip installs from notebooks: %pip install ../../foo.whl
        maybe_library = path_lookup.resolve(library)
        if maybe_library is not None:
            library = maybe_library
        return_code, stdout, stderr = self._runner(f"pip install {library} -t {venv}")
        logger.debug(f"pip output:\n{stdout}\n{stderr}")
        if return_code != 0:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {stderr}")
            return [problem]
        return []

    def __str__(self) -> str:
        return f"PipResolver(whitelist={self._whitelist})"
