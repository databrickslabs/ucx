# pylint: disable=import-outside-toplevel
from __future__ import annotations

import logging
import shlex
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
        self, path_lookup: PathLookup, *libraries: str, installation_arguments: list[str] | None = None
    ) -> list[DependencyProblem]:
        """We delegate to pip to install the library and augment the path look-up to resolve the library at import.
        This gives us the flexibility to install any library that is not in the whitelist, and we don't have to
        bother about parsing cross-version dependencies in our code."""
        compatibility_problems, install_libraries = [], []
        for library in libraries:
            compatibility = self._whitelist.distribution_compatibility(library)
            if compatibility.known:
                compatibility_problems.extend(compatibility.problems)
            else:
                install_libraries.append(library)
        install_problems = []
        if len(install_libraries) > 0:
            install_problems = self._install_library(
                path_lookup, *install_libraries, installation_arguments=installation_arguments or []
            )
        return compatibility_problems + install_problems

    @cached_property
    def _temporary_virtual_environment(self):
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        lib_install_folder = tempfile.mkdtemp(prefix='ucx-')
        return Path(lib_install_folder).resolve()

    def _install_library(
        self, path_lookup: PathLookup, *libraries: str, installation_arguments: list[str]
    ) -> list[DependencyProblem]:
        """Pip install library and augment path look-up to resolve the library at import"""
        path_lookup.append_path(self._temporary_virtual_environment)
        resolved_libraries, resolved_installation_arguments = self._resolve_libraries(
            path_lookup, *libraries, installation_arguments=installation_arguments
        )
        if libraries[0].endswith(".egg"):
            return self._install_egg(*resolved_libraries)
        return self._install_pip(*resolved_libraries, installation_arguments=resolved_installation_arguments)

    @staticmethod
    def _resolve_libraries(
        path_lookup: PathLookup, *libraries: str, installation_arguments: list[str]
    ) -> tuple[list[str], list[str]]:
        # Resolve relative pip installs from notebooks: %pip install ../../foo.whl
        libs, args = [], installation_arguments.copy()
        for library in libraries:
            maybe_library = path_lookup.resolve(Path(library))
            if maybe_library is None:
                libs.append(library)
            else:
                args = [maybe_library.as_posix() if arg == library else arg for arg in args]
                libs.append(maybe_library.as_posix())
        return libs, args

    def _install_pip(self, *libraries: str, installation_arguments: list[str]) -> list[DependencyProblem]:
        problems = []
        venv = self._temporary_virtual_environment
        install_commands = []
        if len(installation_arguments) == 0:
            for library in libraries:
                install_command = f"pip install {shlex.quote(library)} -t {venv}"
                install_commands.append(install_command)
        else:
            # pip allows multiple target directories in its call, it uses the last one, thus the one added here
            install_command = f"pip install {shlex.join(installation_arguments)} -t {venv}"
            install_commands.append(install_command)
            missing_libraries = ",".join([library for library in libraries if library not in installation_arguments])
            if len(missing_libraries) > 0:
                problem = DependencyProblem(
                    "library-install-failed",
                    f"Missing libraries '{missing_libraries}' in installation command '{install_command}'",
                )
                problems.append(problem)
        for install_command in install_commands:
            return_code, stdout, stderr = self._runner(install_command)
            logger.debug(f"pip output:\n{stdout}\n{stderr}")
            if return_code != 0:
                problem = DependencyProblem("library-install-failed", f"'{install_command}' failed with '{stderr}'")
                problems.append(problem)
        return problems

    def _install_egg(self, *libraries: str) -> list[DependencyProblem]:
        """Install eggs using easy_install.

        Sources:
            See easy_install in setuptools.command.easy_install
        """
        problems = []
        if len(libraries) > 1:
            problem = DependencyProblem("not-implemented-yet", "Installing multiple eggs at once")
            problems.append(problem)
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
                return problems
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
            problems.append(problem)
        return problems

    @staticmethod
    def _get_setup() -> Callable | None:
        try:
            # Mypy can't analyze setuptools due to missing type hints
            from setuptools import setup  # type: ignore
        except (ImportError, AssertionError):
            try:
                from distutils.core import setup  # pylint: disable=deprecated-module
            except ImportError:
                logger.warning("Could not import setup.")
                setup = None
        return setup

    def __str__(self) -> str:
        return f"PythonLibraryResolver(whitelist={self._whitelist})"
