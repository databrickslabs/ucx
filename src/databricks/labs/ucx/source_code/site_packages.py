from __future__ import annotations

import os
import subprocess
import tempfile
from functools import cached_property
from pathlib import Path
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    WrappingLoader,
    SourceContainer,
    DependencyGraph,
    BaseDependencyResolver,
    DependencyProblem,
    MaybeDependency,
)


class PipResolver(BaseDependencyResolver):
    # TODO: use DistInfoResolver to load wheel/egg/pypi dependencies
    # TODO: https://github.com/databrickslabs/ucx/issues/1642
    # TODO: https://github.com/databrickslabs/ucx/issues/1643
    # TODO: https://github.com/databrickslabs/ucx/issues/1640

    def __init__(
        self,
        file_loader: FileLoader,
        next_resolver: BaseDependencyResolver | None = None,
    ):
        super().__init__(next_resolver)
        self._file_loader = file_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return PipResolver(self._file_loader, resolver)

    def resolve_library(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        path_lookup.append_path(self._temporary_virtual_environment)
        # invoke pip install via subprocess to install this library into lib_install_folder
        try:
            # TODO: add error handling for pip install
            subprocess.run(['pip', 'install', name, '-t', self._temporary_virtual_environment])
        except Exception as e:  # TODO: catch proper exception
            problem = DependencyProblem('library-install-failed', f'Failed to install {name}: {e}')
            return MaybeDependency(None, [problem])
        dist_infos = SitePackages.parse(self._temporary_virtual_environment)
        dist_info = dist_infos[name]
        if not dist_info:
            problem = DependencyProblem('library-install-failed', f'Failed to install {name}')
            return MaybeDependency(None, [problem])
        container = SitePackageContainer(self._file_loader, dist_info)
        dependency = Dependency(WrappingLoader(container), Path(name))
        return MaybeDependency(dependency, [])

    @cached_property
    def _temporary_virtual_environment(self):
        # TODO: for `databricks labs ucx lint-local-code`, detect if we already have a virtual environment
        # and use that one. See Databricks CLI code for the labs command to see how to detect the virtual
        # environment. If we don't have a virtual environment, create a temporary one.
        # simulate notebook-scoped virtual environment
        lib_install_folder = tempfile.mkdtemp(prefix='ucx-')
        return Path(lib_install_folder)


class SitePackageResolver(BaseDependencyResolver):
    # TODO: this is incorrect logic, remove this resolver

    def __init__(
        self,
        site_packages: SitePackages,
        file_loader: FileLoader,
        path_lookup: PathLookup,
        next_resolver: BaseDependencyResolver | None = None,
    ):
        super().__init__(next_resolver)
        self._site_packages = site_packages
        self._file_loader = file_loader
        self._path_lookup = path_lookup

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return SitePackageResolver(self._site_packages, self._file_loader, self._path_lookup, resolver)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        # TODO: `resovle_import` is irrelevant for dist-info containers
        # databricks-labs-ucx vs databricks.labs.ucx
        site_package = self._site_packages[name]
        if site_package is not None:
            container = SitePackageContainer(self._file_loader, site_package)
            dependency = Dependency(WrappingLoader(container), Path(name))
            return MaybeDependency(dependency, [])
        return super().resolve_import(path_lookup, name)


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


class SitePackages:

    @classmethod
    def parse(cls, site_packages_path: Path) -> "SitePackages":
        dist_info_dirs = [dir for dir in os.listdir(site_packages_path) if dir.endswith(".dist-info")]
        packages = [SitePackage.parse(Path(site_packages_path, dist_info_dir)) for dist_info_dir in dist_info_dirs]
        return cls(packages)

    def __init__(self, packages: list[SitePackage]):
        self._packages: dict[str, SitePackage] = {}
        for package in packages:
            for top_level in package.top_levels:
                self._packages[top_level] = package

    def __getitem__(self, item: str) -> SitePackage | None:
        item = item.replace("-", "_")
        return self._packages.get(item, None)


class SitePackage:

    @classmethod
    def parse(cls, path: Path) -> "SitePackage":
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
        return cls(path, top_levels, [Path(module) for module in modules])

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
