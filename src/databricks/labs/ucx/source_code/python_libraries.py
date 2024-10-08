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
from databricks.labs.ucx.source_code.known import KnownList

logger = logging.getLogger(__name__)


class PythonLibraryResolver(LibraryResolver):
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(
        self, allow_list: KnownList, runner: Callable[[str | list[str]], tuple[int, str, str]] = run_command
    ) -> None:
        self._allow_list = allow_list
        self._runner = runner

    def register_library(self, path_lookup: PathLookup, *libraries: str) -> list[DependencyProblem]:
        """We delegate to pip to install the library and augment the path look-up to resolve the library at import.
        This gives us the flexibility to install any library that is not in the allow-list, and we don't have to
        bother about parsing cross-version dependencies in our code."""
        if len(libraries) == 0:
            return []
        if len(libraries) == 1:  # Multiple libraries might be installation flags
            compatibility = self._allow_list.distribution_compatibility(libraries[0])
            if compatibility.known:
                return compatibility.problems
        return self._install_library(path_lookup, *libraries)

    @cached_property
    def _temporary_virtual_environment(self) -> Path:
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        lib_install_folder = tempfile.mkdtemp(prefix='ucx-')
        return Path(lib_install_folder).resolve()

    def _install_library(self, path_lookup: PathLookup, *libraries: str) -> list[DependencyProblem]:
        """Pip install library and augment path look-up to resolve the library at import"""
        path_lookup.append_path(self._temporary_virtual_environment)
        resolved_libraries = self._resolve_libraries(path_lookup, *libraries)
        if libraries[0].endswith(".egg"):
            return self._install_egg(*resolved_libraries)
        return self._install_pip(*resolved_libraries)

    @staticmethod
    def _resolve_libraries(path_lookup: PathLookup, *libraries: str) -> list[str]:
        # Resolve relative pip installs from notebooks: %pip install ../../foo.whl
        libs = []
        for library in libraries:
            maybe_library = path_lookup.resolve(Path(library))
            if maybe_library is None:
                libs.append(library)
            else:
                libs.append(maybe_library.as_posix())
        return libs

    def _install_pip(self, *libraries: str) -> list[DependencyProblem]:
        args = [
            "pip",
            "--disable-pip-version-check",
            "install",
            *libraries,
            "-t",
            str(self._temporary_virtual_environment),
        ]
        return_code, stdout, stderr = self._runner(args)
        logger.debug(f"pip output:\n{stdout}\n{stderr}")
        if return_code != 0:
            command = " ".join(args)
            problem = DependencyProblem("library-install-failed", f"'{command}' failed with '{stderr}'")
            return [problem]
        return []

    def _install_egg(self, *libraries: str) -> list[DependencyProblem]:
        """Install eggs using easy_install.

        Sources:
            See easy_install in setuptools.command.easy_install
        """
        if len(libraries) > 1:
            problems = []
            for library in libraries:
                problems.extend(self._install_egg(library))
            return problems
        library = libraries[0]

        verbosity = "--verbose" if is_in_debug() else "--quiet"
        easy_install_arguments = [
            "easy_install",
            verbosity,
            "--always-unzip",
            "--install-dir",
            self._temporary_virtual_environment.as_posix(),
            library,
        ]
        setup = self._get_setup()
        if callable(setup):
            try:
                setup(script_args=easy_install_arguments)
                return []
            except (SystemExit, ImportError, ValueError) as e:
                logger.warning(f"Failed to install {library} with (setuptools|distutils).setup, unzipping instead: {e}")
        library_folder = self._temporary_virtual_environment / Path(library).name
        library_folder.mkdir(parents=True, exist_ok=True)
        try:
            # Not a "real" install, though, the best effort to still lint eggs without dependencies
            with zipfile.ZipFile(library, "r") as zip_ref:
                zip_ref.extractall(library_folder)
        except (zipfile.BadZipfile, FileNotFoundError) as e:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")
            return [problem]
        return []

    @staticmethod
    def _get_setup() -> Callable | None:
        try:
            # Mypy can't analyze setuptools due to missing type hints
            from setuptools import setup  # type: ignore
        except (ImportError, AssertionError):
            try:
                # pylint: disable-next=deprecated-module
                from distutils.core import setup  # type: ignore
            except ImportError:
                logger.warning("Could not import setup.")
                setup = None
        return setup

    def __str__(self) -> str:
        return f"PythonLibraryResolver(allow_list={self._allow_list})"
