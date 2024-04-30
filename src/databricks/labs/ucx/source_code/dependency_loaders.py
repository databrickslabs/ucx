from __future__ import annotations

import abc
import typing
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat, Language

from databricks.labs.ucx.source_code.base import NOTEBOOK_HEADER
from databricks.labs.ucx.source_code.files import LocalFile
from databricks.labs.ucx.source_code.syspath_provider import SysPathProvider

from databricks.labs.ucx.source_code.dependency_containers import SourceContainer
from databricks.labs.ucx.source_code.dependency_graph import Dependency
from databricks.labs.ucx.source_code.notebook import Notebook

if typing.TYPE_CHECKING:
    from databricks.labs.ucx.source_code.dependency_graph import DependencyGraph


class StubContainer(SourceContainer):

    def build_dependency_graph(self, parent: DependencyGraph, syspath_provider: SysPathProvider) -> None:
        pass


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

    def __init__(self, syspath_provider: SysPathProvider):
        self._syspath_provider = syspath_provider

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        fullpath = self.full_path(dependency.path)
        assert fullpath is not None
        return LocalFile(fullpath, fullpath.read_text("utf-8"), Language.PYTHON)

    def is_file(self, path: Path) -> bool:
        return self.full_path(path) is not None

    def full_path(self, path: Path) -> Path | None:
        if path.is_file():
            return path
        child = Path(self._syspath_provider.cwd, path)
        if child.is_file():
            return child
        for parent in self._syspath_provider.paths:
            child = Path(parent, path)
            if child.is_file():
                return child
        return None

    def is_notebook(self, path: Path) -> bool:
        fullpath = self.full_path(path)
        if fullpath is None:
            return False
        with fullpath.open(mode="r", encoding="utf-8") as stream:
            line = stream.readline()
            return NOTEBOOK_HEADER in line


class NotebookLoader(DependencyLoader, abc.ABC):
    pass


class LocalNotebookLoader(NotebookLoader, LocalFileLoader):
    # see https://github.com/databrickslabs/ucx/issues/1499
    # pylint: disable=stuff
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        fullpath = self.full_path(dependency.path)
        assert fullpath is not None
        return Notebook.parse(fullpath, fullpath.read_text("utf-8"), Language.PYTHON)


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
        assert object_info.path is not None
        assert object_info.language is not None
        source = self._load_source(object_info)
        return Notebook.parse(Path(object_info.path), source, object_info.language)

    def _load_source(self, object_info: ObjectInfo) -> str:
        assert object_info.path is not None
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")
