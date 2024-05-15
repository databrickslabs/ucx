from __future__ import annotations

import os
import subprocess
import tempfile
from functools import cached_property
from pathlib import Path
from subprocess import CalledProcessError

from databricks.labs.ucx.source_code.graph import BaseLibraryInstaller, DependencyProblem
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class PipInstaller(BaseLibraryInstaller):
    # TODO: use DistInfoResolver to load wheel/egg/pypi dependencies
    # TODO: https://github.com/databrickslabs/ucx/issues/1642
    # TODO: https://github.com/databrickslabs/ucx/issues/1643
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(self, next_installer: BaseLibraryInstaller | None = None) -> None:
        super().__init__(next_installer)

    def with_next_installer(self, installer: BaseLibraryInstaller) -> PipInstaller:
        return PipInstaller(self._next_installer)

    def install_library_pip(self, path_lookup: PathLookup, library: str) -> list[DependencyProblem]:
        """Pip install library and augment path look-up so that is able to resolve the library"""
        # invoke pip install via subprocess to install this library into lib_install_folder
        pip_install_arguments = ["pip", "install", library, "-t", self._temporary_virtual_environment.as_posix()]
        try:
            subprocess.run(pip_install_arguments, check=True)
        except CalledProcessError as e:
            return [DependencyProblem("library-install-failed", f"Failed to install {library}: {e}")]

        path_lookup.append_path(self._temporary_virtual_environment)
        return []

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


class SitePackages:

    @staticmethod
    def parse(site_packages_path: Path):
        dist_info_dirs = [dir for dir in os.listdir(site_packages_path) if dir.endswith(".dist-info")]
        packages = [SitePackage.parse(Path(site_packages_path, dist_info_dir)) for dist_info_dir in dist_info_dirs]
        return SitePackages(packages)

    def __init__(self, packages: list[SitePackage]):
        self._packages: dict[str, SitePackage] = {}
        for package in packages:
            for top_level in package.top_levels:
                self._packages[top_level] = package

    def __getitem__(self, item: str) -> SitePackage | None:
        # TODO: Replace hyphen with underscores
        # TODO: Don't use get, raise KeyError
        return self._packages.get(item, None)


class SitePackage:

    @staticmethod
    def parse(path: Path):
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
        return SitePackage(path, top_levels, [Path(module) for module in modules])

    def __init__(self, dist_info_path: Path, top_levels: list[str], module_paths: list[Path]):
        self._dist_info_path = dist_info_path
        self._top_levels = top_levels
        self._module_paths = module_paths

    @property
    def top_levels(self) -> list[str]:
        return self._top_levels

    @property
    def module_paths(self) -> list[Path]:
        return [Path(self._dist_info_path.parent, path) for path in self._module_paths]

    def __repr__(self):
        return f"<SitePackage {self._dist_info_path}>"
