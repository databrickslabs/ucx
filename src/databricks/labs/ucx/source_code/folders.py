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
    """A source container that represents a folder.

    Args:
        ignore_relative_child_paths (set[Path] | None) : The child path relative
            to this folder to be ignored while building a dependency graph.
            If None, no child paths are ignored.

            NOTE: The child paths are only ignored during dependency graph
            building of **this** folder. If a child path is referenced via
            another route, like an import of the file by a non-ignored file,
            then a dependency for that child will be still be added to the
            graph.
    """

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
        *,
        ignore_relative_child_paths: set[Path] | None = None,
    ):
        self._path = path
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._folder_loader = folder_loader
        self._ignore_relative_child_paths = ignore_relative_child_paths or set[Path]()

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
            if child_path.relative_to(self._path) in self._ignore_relative_child_paths:
                continue
            is_file = child_path.is_file()
            is_notebook = is_a_notebook(child_path)
            loader = self._notebook_loader if is_notebook else self._file_loader if is_file else self._folder_loader
            dependency = Dependency(loader, child_path, inherits_context=is_notebook)
            yield from parent.register_dependency(dependency).problems

    def __repr__(self):
        return f"<Folder {self._path}>"


class FolderLoader(DependencyLoader):
    """Load a folder.

    Args:
        ignore_relative_child_paths (set[Path] | None) : see :class:Folder.
    """

    def __init__(
        self,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        *,
        ignore_relative_child_paths: set[Path] | None = None,
    ):
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._ignore_relative_child_paths = ignore_relative_child_paths or set[Path]()

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> Folder | None:
        """Load the folder as a dependency."""
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        folder = Folder(
            absolute_path,
            self._notebook_loader,
            self._file_loader,
            self,
            ignore_relative_child_paths=self._ignore_relative_child_paths,
        )
        return folder
