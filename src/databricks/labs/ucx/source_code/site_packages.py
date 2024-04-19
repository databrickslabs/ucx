import os
from dataclasses import dataclass
from pathlib import Path

from databricks.labs.ucx.source_code.dependencies import (
    SourceContainer,
    DependencyType,
    DependencyGraph,
    UnresolvedDependency,
    PackageFileDependency,
)


@dataclass
class SitePackage(SourceContainer):

    @classmethod
    def parse(cls, path: Path):
        with open(Path(path, "RECORD"), encoding="utf-8") as record_file:
            lines = record_file.readlines()
            files = [line.split(',')[0] for line in lines]
            modules = list(filter(lambda line: line.endswith(".py"), files))
        top_levels_path = Path(path, "top_level.txt")
        if top_levels_path.exists():
            with open(top_levels_path, encoding="utf-8") as top_levels_file:
                top_levels = [line.strip() for line in top_levels_file.readlines()]
        else:
            dir_name = cls._dir_name_from_distinfo_name(path.name)
            top_levels = [dir_name]
        return SitePackage(path, top_levels, modules)

    @staticmethod
    def _dir_name_from_distinfo_name(distinfo: str):
        # strip extension
        dir_name = distinfo[: distinfo.rindex('.')]
        # strip version
        return dir_name[: dir_name.rindex('-')]

    def __init__(self, dist_info_path: Path, top_levels: list[str], module_paths: list[str]):
        self._dist_info_path = dist_info_path
        self._top_levels = top_levels
        self._module_paths = module_paths

    @property
    def top_levels(self) -> list[str]:
        return self._top_levels

    @property
    def dependency_type(self) -> DependencyType:
        return DependencyType.PACKAGE

    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        for module_path in self._module_paths:
            parent.register_dependency(PackageFileDependency(self, module_path))

    def load_module_source_code(self, path: str):
        source_path = Path(self._dist_info_path.parent, path)
        with source_path.open("r", encoding="utf-8") as f:
            return f.read()

    def register_dependency(self, graph: DependencyGraph, path: str):
        for top_level in self._top_levels:
            module_path = f"{top_level}/{path}.py"
            if module_path in self._module_paths:
                return graph.register_dependency(PackageFileDependency(self, module_path))
        return graph.register_dependency(UnresolvedDependency(path))


class PackageFile(SourceContainer):

    def __init__(self, package: SitePackage, path: str):
        self._package = package
        self._path = path
        self._source_code: str | None = None

    @property
    def dependency_type(self) -> DependencyType:
        return DependencyType.PACKAGE_FILE

    def _load_source_code(self):
        if self._source_code is None:
            self._source_code = self._package.load_module_source_code(self._path)

    def build_dependency_graph(self, parent: DependencyGraph):
        self._load_source_code()
        assert self._source_code is not None
        parent.build_graph_from_python_source(
            self._source_code, lambda name: self._package.register_dependency(parent, name)
        )


class SitePackages:

    @staticmethod
    def parse(site_packages_path: str):
        dist_info_dirs = [dir for dir in os.listdir(site_packages_path) if dir.endswith(".dist-info")]
        packages = [SitePackage.parse(Path(site_packages_path, dist_info_dir)) for dist_info_dir in dist_info_dirs]
        return SitePackages(packages)

    def __init__(self, packages: list[SitePackage]):
        self._packages: dict[str, SitePackage] = {}
        for package in packages:
            for top_level in package.top_levels:
                self._packages[top_level] = package

    def __getitem__(self, item: str) -> SitePackage | None:
        return self._packages.get(item, None)
