# pylint: disable=import-outside-toplevel
from __future__ import annotations

import logging
import tempfile
import zipfile
from collections.abc import Callable
from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.entrypoint import is_in_debug

from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.source_code.graph import (
    LibraryResolver,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.known import Whitelist

logger = logging.getLogger(__name__)


class PythonLibraryResolver(LibraryResolver):
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(self, whitelist: Whitelist, runner: Callable[[str], tuple[int, str, str]] = run_command) -> None:
        self._whitelist = whitelist
        self._runner = runner

    def register_library(
        self, path_lookup: PathLookup, library: Path, *, installation_parameters: dict[str, str] | None = None
    ) -> list[DependencyProblem]:
        """We delegate to pip to install the library and augment the path look-up to resolve the library at import.
        This gives us the flexibility to install any library that is not in the whitelist, and we don't have to
        bother about parsing cross-version dependencies in our code."""
        compatibility = self._whitelist.distribution_compatibility(library.name)
        if compatibility.known:
            return compatibility.problems
        return self._install_library(path_lookup, library, installation_parameters=installation_parameters or dict())

    @cached_property
    def _temporary_virtual_environment(self):
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        lib_install_folder = tempfile.mkdtemp(prefix='ucx-')
        return Path(lib_install_folder).resolve()

    def _install_library(
        self, path_lookup: PathLookup, library: Path, *, installation_parameters: dict[str, str]
    ) -> list[DependencyProblem]:
        """Pip install library and augment path look-up to resolve the library at import"""
        path_lookup.append_path(self._temporary_virtual_environment)

        # Resolve relative pip installs from notebooks: %pip install ../../foo.whl
        maybe_library = path_lookup.resolve(library)
        if maybe_library is not None:
            library = maybe_library

        if library.suffix == ".egg":
            return self._install_egg(library)
        return self._install_pip(library, installation_parameters=installation_parameters)

    def _install_pip(self, library: Path, *, installation_parameters: dict[str, str]) -> list[DependencyProblem]:
        return_code, stdout, stderr = self._runner(f"pip install {library} -t {self._temporary_virtual_environment}")
        logger.debug(f"pip output:\n{stdout}\n{stderr}")
        if return_code != 0:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {stderr}")
            return [problem]
        return []

    def _install_egg(self, library: Path) -> list[DependencyProblem]:
        """Install egss using easy_install.

        Sources:
            See easy_install in setuptools.command.easy_install
        """
        verbosity = "--verbose" if is_in_debug() else "--quiet"
        easy_install_arguments = [
            "easy_install",
            verbosity,
            "--always-unzip",
            "--install-dir",
            self._temporary_virtual_environment.as_posix(),
            library.as_posix(),
        ]
        setup = self._get_setup()
        if callable(setup):
            try:
                setup(script_args=easy_install_arguments)
                return []
            except (SystemExit, ImportError, ValueError) as e:
                logger.warning(f"Failed to install {library} with (setuptools|distutils).setup, unzipping instead: {e}")
        library_folder = self._temporary_virtual_environment / library.name
        library_folder.mkdir(parents=True, exist_ok=True)
        try:
            # Not a "real" install, though, the best effort to still lint eggs without dependencies
            with zipfile.ZipFile(library, "r") as zip_ref:
                zip_ref.extractall(library_folder)
        except zipfile.BadZipfile as e:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")
            return [problem]
        return []

    @staticmethod
    def _get_setup() -> Callable | None:
        try:
            # Mypy can't analyze setuptools due to missing type hints
            from setuptools import setup  # type: ignore
        except ImportError:
            try:
                from distutils.core import setup  # pylint: disable=deprecated-module
            except ImportError:
                logger.warning("Could not import setup.")
                setup = None
        return setup

    def __str__(self) -> str:
        return f"PythonLibraryResolver(whitelist={self._whitelist})"
