from __future__ import annotations

import abc
import typing

from databricks.labs.ucx.source_code.dependency_graph import DependencyGraph, Dependency

if typing.TYPE_CHECKING:
    from databricks.labs.ucx.source_code.dependency_loaders import LocalFileLoader
    from databricks.labs.ucx.source_code.site_packages import SitePackage


class SourceContainer(abc.ABC):

    @abc.abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        raise NotImplementedError()


class SitePackageContainer(SourceContainer):

    def __init__(self, file_loader: LocalFileLoader, site_package: SitePackage):
        self._file_loader = file_loader
        self._site_package = site_package

    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        for module_path in self._site_package.module_paths:
            parent.register_dependency(Dependency(self._file_loader, module_path))
