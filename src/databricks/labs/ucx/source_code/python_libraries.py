from __future__ import annotations

import os
import subprocess
import tempfile
from functools import cached_property
from pathlib import Path
from subprocess import CalledProcessError

from databricks.labs.ucx.source_code.graph import BaseLibraryResolver, DependencyProblem, MaybeDependency
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class PipResolver(BaseLibraryResolver):
    # TODO: use DistInfoResolver to load wheel/egg/pypi dependencies
    # TODO: https://github.com/databrickslabs/ucx/issues/1642
    # TODO: https://github.com/databrickslabs/ucx/issues/1643
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(self, next_resolver: BaseLibraryResolver | None = None) -> None:
        super().__init__(next_resolver)

    def with_next_resolver(self, resolver: BaseLibraryResolver) -> PipResolver:
        return PipResolver(resolver)

    def resolve_library(self, path_lookup: PathLookup, library: str) -> MaybeDependency:
        """Pip install library and augment path look-up to resolve the library at import"""
        # invoke pip install via subprocess to install this library into lib_install_folder
        pip_install_arguments = ["pip", "install", library, "-t", self._temporary_virtual_environment.as_posix()]
        try:
            subprocess.run(pip_install_arguments, check=True)
        except CalledProcessError as e:
            problem = DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")
            return MaybeDependency(None, [problem])

        path_lookup.append_path(self._temporary_virtual_environment)
        return MaybeDependency(None, [])

    @cached_property
    def _temporary_virtual_environment(self) -> Path:
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        lib_install_folder = tempfile.mkdtemp(prefix="ucx-")
        return Path(lib_install_folder)


COMMENTED_OUT_FOR_PR_1685 = """
class SitePackageContainer(SourceContainer):

    def __init__(self, file_loader: FileLoader, site_package: SitePackage):
        self._file_loader = file_loader
        self._site_package = site_package

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        problems: list[DependencyProblem] = []
        for module_path in self._site_package.module_paths:
            maybe = parent.register_dependency(Dependency(self._file_loader, module_path))
            if maybe.problems:
                problems.extend(maybe.problems)
        return problems

    @property
    def paths(self):
        return self._site_package.module_paths

    def __repr__(self):
        return f"<SitePackageContainer {self._site_package}>"
"""

REQUIRES_DIST_PREFIX = "Requires-Dist: "


class DistInfoPackage:
    """
    represents a wheel package installable via pip
    see https://packaging.python.org/en/latest/specifications/binary-distribution-format/
    """
    @classmethod
    def parse(cls, path: Path):
        # not using importlib.metadata because it only works on accessible packages
        # which we can't guarantee since we have our own emulated sys.paths
        with open(Path(path, "METADATA"), encoding="utf-8") as metadata_file:
            lines = metadata_file.readlines()
            lines = filter(lambda line: line.startswith(REQUIRES_DIST_PREFIX), lines)
            library_names = [cls._extract_library_name_from_requires_dist(line[len(REQUIRES_DIST_PREFIX):]) for line in lines]
        with open(Path(path, "RECORD"), encoding="utf-8") as record_file:
            lines = record_file.readlines()
            files = [line.split(',')[0] for line in lines]
            modules = list(filter(lambda line: line.endswith(".py"), files))
        top_levels_path = Path(path, "top_level.txt")
        if top_levels_path.exists():
            with open(top_levels_path, encoding="utf-8") as top_levels_file:
                top_levels = [line.strip() for line in top_levels_file.readlines()]
        else:
            dir_name = path.name
            # strip extension
            dir_name = dir_name[: dir_name.rindex('.')]
            # strip version
            dir_name = dir_name[: dir_name.rindex('-')]
            top_levels = [dir_name]
        return DistInfoPackage(path, top_levels, [Path(module) for module in modules], library_names)

    @classmethod
    def _extract_library_name_from_requires_dist(cls, spec: str) -> str:
        delimiters = { ' ', '@', '<', '>', ';' }
        for i, c in enumerate(spec):
            if c in delimiters:
                return spec[:i]
        return spec

    def __init__(self, dist_info_path: Path, top_levels: list[str], module_paths: list[Path], library_names: list[str]):
        self._dist_info_path = dist_info_path
        self._top_levels = top_levels
        self._module_paths = module_paths
        self._library_names = library_names

    @property
    def top_levels(self) -> list[str]:
        return self._top_levels

    @property
    def module_paths(self) -> list[Path]:
        return [Path(self._dist_info_path.parent, path) for path in self._module_paths]

    @property
    def library_names(self) -> list[str]:
        return self._library_names

    def __repr__(self):
        return f"<DistInfoPackage {self._dist_info_path}>"
