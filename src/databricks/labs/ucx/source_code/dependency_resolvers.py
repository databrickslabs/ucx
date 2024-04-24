from __future__ import annotations

import typing
from collections.abc import Iterable

from pathlib import Path

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.labs.ucx.source_code.dependencies import Dependency
from databricks.labs.ucx.source_code.dependency_loaders import SitePackageContainer, WrappingLoader
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility

if typing.TYPE_CHECKING:
    from databricks.labs.ucx.source_code.dependency_loaders import LocalFileLoader, NotebookLoader


class DependencyResolver:
    def __init__(
        self,
        whitelist: Whitelist,
        site_packages: SitePackages,
        file_loader: LocalFileLoader,
        notebook_loader: NotebookLoader,
    ):
        self._whitelist = whitelist
        self._site_packages = site_packages
        self._file_loader = file_loader
        self._notebook_loader = notebook_loader
        self._advices: list[Advice] = []

    def resolve_notebook(self, path: Path) -> Dependency | None:
        if self._notebook_loader.is_notebook(path):
            return Dependency(self._notebook_loader, path)
        return None

    def resolve_local_file(self, path: Path) -> Dependency | None:
        if self._file_loader.is_file(path) and not self._file_loader.is_notebook(path):
            return Dependency(self._file_loader, path)
        return None

    def resolve_import(self, name: str) -> Dependency | None:
        if self._is_whitelisted(name):
            return None
        if self._file_loader.is_file(Path(name)):
            return Dependency(self._file_loader, Path(name))
        site_package = self._site_packages[name]
        if site_package is not None:
            container = SitePackageContainer(self._file_loader, site_package)
            return Dependency(WrappingLoader(container), Path(name))
        raise ValueError(f"Could not locate {name}")

    def _is_whitelisted(self, name: str) -> bool:
        compatibility = self._whitelist.compatibility(name)
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        if compatibility is None:
            return False
        if compatibility == UCCompatibility.NONE:
            # TODO this should be done as part of linting, not as part of dependency graph building
            self._advices.append(
                Deprecation(
                    code="dependency-check",
                    message=f"Use of dependency {name} is deprecated",
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            )
        return True

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices
