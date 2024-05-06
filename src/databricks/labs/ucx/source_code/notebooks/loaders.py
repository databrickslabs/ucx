from __future__ import annotations

import abc
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat, Language

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import (
    BaseDependencyResolver,
    Dependency,
    DependencyLoader,
    SourceContainer,
    MaybeDependency,
)
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class NotebookResolver(BaseDependencyResolver):

    def __init__(self, notebook_loader: NotebookLoader, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._notebook_loader = notebook_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return NotebookResolver(self._notebook_loader, resolver)

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        absolute_path = self._notebook_loader.resolve(path_lookup, path)
        if not absolute_path:
            return super().resolve_notebook(path_lookup, path)
        dependency = Dependency(self._notebook_loader, absolute_path)
        return MaybeDependency(dependency, [])


class NotebookLoader(DependencyLoader, abc.ABC):
    def resolve(self, path_lookup: PathLookup, path: Path) -> Path | None:
        """Resolve the path to a notebook file. If the path is not a notebook, return None."""
        return path_lookup.resolve(path)


class WorkspaceNotebookLoader(NotebookLoader):

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def is_notebook(self, path: Path):
        object_info = self._ws.workspace.get_status(str(path))
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        return object_info is not None and object_info.object_type is ObjectType.NOTEBOOK

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
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

    def __repr__(self):
        return f"<WorkspaceNotebookLoader ws={self._ws}>"


class LocalNotebookLoader(NotebookLoader, FileLoader):

    def resolve(self, path_lookup: PathLookup, path: Path) -> Path | None:
        """When exported through Git, notebooks are saved with a .py extension. If the path is a notebook, return the
        path to the notebook. If the path is a Python file, return the path to the Python file. If the path is neither,
        return None."""
        for path in (self._adjust_path(path), path):
            absolute_path = path_lookup.resolve(path)
            if not absolute_path:
                continue
            return absolute_path
        return None

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        # TODO: catch exceptions, and create MaybeContainer to follow the pattern - OSError and NotFound are common
        absolute_path = self.resolve(path_lookup, dependency.path)
        if not absolute_path:
            return None
        return Notebook.parse(absolute_path, absolute_path.read_text("utf-8"), Language.PYTHON)

    @staticmethod
    def _adjust_path(path: Path):
        if path.suffix == ".py":
            return path
        return Path(path.as_posix() + ".py")

    def __repr__(self):
        return "LocalNotebookLoader()"
