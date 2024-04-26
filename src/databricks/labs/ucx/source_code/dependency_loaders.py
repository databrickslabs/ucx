from __future__ import annotations

import abc
import typing
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat

from databricks.labs.ucx.source_code.dependencies import Dependency


if typing.TYPE_CHECKING:
    from databricks.labs.ucx.source_code.site_packages import SitePackage
    from databricks.labs.ucx.source_code.dependencies import DependencyGraph


class SourceContainer(abc.ABC):

    @abc.abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        raise NotImplementedError()


class DependencyLoader(abc.ABC):

    @abc.abstractmethod
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()


# a DependencyLoader that simply wraps a pre-existing SourceContainer
class WrappingLoader(DependencyLoader):

    def __init__(self, source_container: SourceContainer):
        self._source_container = source_container

    def is_file(self, path: Path) -> bool:
        raise NotImplementedError()  # should never happen

    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()  # should never happen

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        return self._source_container


class LocalFileLoader(DependencyLoader):
    # see https://github.com/databrickslabs/ucx/issues/1499
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()

    def is_file(self, path: Path) -> bool:
        raise NotImplementedError()

    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()


class NotebookLoader(DependencyLoader, abc.ABC):
    pass


class LocalNotebookLoader(NotebookLoader, LocalFileLoader):
    # see https://github.com/databrickslabs/ucx/issues/1499
    pass


class SitePackageContainer(SourceContainer):

    def __init__(self, file_loader: LocalFileLoader, site_package: SitePackage):
        self._file_loader = file_loader
        self._site_package = site_package

    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        for module_path in self._site_package.module_paths:
            parent.register_dependency(Dependency(self._file_loader, module_path))


class WorkspaceNotebookLoader(NotebookLoader):

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def is_notebook(self, path: Path):
        object_info = self._ws.workspace.get_status(str(path))
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        return object_info is not None and object_info.object_type is ObjectType.NOTEBOOK

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        object_info = self._ws.workspace.get_status(str(dependency.path))
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        return self._load_notebook(object_info)

    def _load_notebook(self, object_info: ObjectInfo) -> SourceContainer:
        # local import to avoid cyclic dependency
        # pylint: disable=import-outside-toplevel, cyclic-import
        from databricks.labs.ucx.source_code.notebook import Notebook

        assert object_info.path is not None
        assert object_info.language is not None
        source = self._load_source(object_info)
        return Notebook.parse(object_info.path, source, object_info.language)

    def _load_source(self, object_info: ObjectInfo) -> str:
        assert object_info.path is not None
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")
