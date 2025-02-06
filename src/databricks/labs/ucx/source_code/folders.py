from __future__ import annotations

from pathlib import Path
from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import is_a_notebook
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyLoader,
    DependencyProblem,
    SourceContainer,
)
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class Folder(SourceContainer):
    """A source container that represents a folder."""

    # The following paths names are ignore as they do not contain source code
    _IGNORE_PATH_NAMES = {
        "__pycache__",
        ".mypy_cache",
        ".git",
        ".github",
        # Code from libraries are accessed through `imports`, not directly via the folder
        ".venv",
        "site-packages",
    }

    def __init__(
        self,
        path: Path,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
    ):
        self._path = path
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._folder_loader = folder_loader

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        """Build the dependency graph for the folder.

        Here we skip certain directories, like:
        - the ones that are not source code.
        """
        if self._path.name in self._IGNORE_PATH_NAMES:
            return []
        return list(self._build_dependency_graph(parent))

    def _build_dependency_graph(self, parent: DependencyGraph) -> Iterable[DependencyProblem]:
        """Build the dependency graph for the contents of the folder."""
        for child_path in self._path.iterdir():
            is_file = child_path.is_file()
            is_notebook = is_a_notebook(child_path)
            loader = self._notebook_loader if is_notebook else self._file_loader if is_file else self._folder_loader
            dependency = Dependency(loader, child_path, inherits_context=is_notebook)
            yield from parent.register_dependency(dependency).problems

    def __repr__(self):
        return f"<Folder {self._path}>"


class FolderLoader(DependencyLoader):
    """Load a folder."""

    def __init__(self, notebook_loader: NotebookLoader, file_loader: FileLoader):
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> Folder | None:
        """Load the folder as a dependency."""
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        return Folder(absolute_path, self._notebook_loader, self._file_loader, self)
