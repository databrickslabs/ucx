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
        return_code, _, stderr = self._runner(f"pip install {library} -t {venv}")
        if return_code != 0:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {stderr}")
            return [problem]
        return []
