from __future__ import annotations

import logging
import tempfile
from collections.abc import Callable
from functools import cached_property
from pathlib import Path

from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.source_code.graph import (
    LibraryResolver,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.known import Whitelist

logger = logging.getLogger(__name__)


class PipResolver(LibraryResolver):
    # TODO: https://github.com/databrickslabs/ucx/issues/1643
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(self, whitelist: Whitelist, runner: Callable[[str], tuple[int, str, str]] = run_command) -> None:
        self._whitelist = whitelist
        self._runner = runner

    def register_library(self, path_lookup: PathLookup, library: Path) -> list[DependencyProblem]:
        """We delegate to pip to install the library and augment the path look-up to resolve the library at import.
        This gives us the flexibility to install any library that is not in the whitelist, and we don't have to
        bother about parsing cross-version dependencies in our code."""
        compatibility = self._whitelist.distribution_compatibility(library.name)
        if compatibility.known:
            return compatibility.problems
        return self._install_library(path_lookup, library)

    @cached_property
    def _temporary_virtual_environment(self):
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
