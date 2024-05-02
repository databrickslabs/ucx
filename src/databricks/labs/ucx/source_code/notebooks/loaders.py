from __future__ import annotations

import abc
from pathlib import Path
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat, Language

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import (
    BaseDependencyResolver,
    DependencyProblem,
    Dependency,
    DependencyLoader,
    SourceContainer,
)
from databricks.labs.ucx.source_code.notebooks.sources import Notebook


class NotebookResolver(BaseDependencyResolver):

    def __init__(self, notebook_loader: NotebookLoader, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._notebook_loader = notebook_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return NotebookResolver(self._notebook_loader, resolver)

    def resolve_notebook(self, path: Path, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        if self._notebook_loader.is_notebook(path):
            return Dependency(self._notebook_loader, path)
        return super().resolve_notebook(path, problem_collector)


class NotebookLoader(DependencyLoader, abc.ABC):
    pass


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


class LocalNotebookLoader(NotebookLoader, FileLoader):

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        fullpath = self.full_path(self._adjust_path(dependency.path))
        assert fullpath is not None
        return Notebook.parse(fullpath, fullpath.read_text("utf-8"), Language.PYTHON)

    def is_notebook(self, path: Path) -> bool:
        return super().is_notebook(self._adjust_path(path))

    @staticmethod
    def _adjust_path(path: Path):
        if path.suffix == ".py":
            return path
        return Path(path.as_posix() + ".py")
