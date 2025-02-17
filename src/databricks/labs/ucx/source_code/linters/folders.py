from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from pathlib import Path

from databricks.labs.ucx.source_code.base import LocatedAdvice, is_a_notebook
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.folders import FolderLoader
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyLoader,
    DependencyProblem,
    DependencyResolver,
    MaybeGraph,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.graph_walkers import FixerWalker, LinterWalker
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup


logger = logging.getLogger(__name__)


class LocalCodeLinter:
    """Lint local code to become Unity Catalog compatible."""

    def __init__(
        self,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
        path_lookup: PathLookup,
        dependency_resolver: DependencyResolver,
        context_factory: Callable[[], LinterContext],
    ) -> None:
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._folder_loader = folder_loader
        self._path_lookup = path_lookup
        self._dependency_resolver = dependency_resolver
        self._context_factory = context_factory

    def lint(self, path: Path) -> Iterable[LocatedAdvice]:
        """Lint local code generating advices on becoming Unity Catalog compatible.

        Parameters :
            path (Path) : The path to the resource(s) to lint. If the path is a directory, then all files within the
                directory and subdirectories are linted.
        """
        maybe_graph = self._build_dependency_graph_from_path(path)
        if maybe_graph.problems:
            for problem in maybe_graph.problems:
                yield problem.as_located_advice()
            return
        assert maybe_graph.graph
        walker = LinterWalker(maybe_graph.graph, self._path_lookup, self._context_factory)
        yield from walker

    def apply(self, path: Path) -> Iterable[LocatedAdvice]:
        """Apply local code fixes to become Unity Catalog compatible.

        Parameters :
            path (Path) : The path to the resource(s) to lint. If the path is a directory, then all files within the
                directory and subdirectories are linted.
        """
        maybe_graph = self._build_dependency_graph_from_path(path)
        if maybe_graph.problems:
            for problem in maybe_graph.problems:
                yield problem.as_located_advice()
            return
        assert maybe_graph.graph
        walker = FixerWalker(maybe_graph.graph, self._path_lookup, self._context_factory)
        list(walker)  # Nothing to yield

    def _build_dependency_graph_from_path(self, path: Path) -> MaybeGraph:
        """Build a dependency graph from the path.

        It tries to load the path as a directory, file or notebook.

        Returns :
            MaybeGraph : If the loading fails, the returned maybe graph contains a problem. Otherwise, returned maybe
            graph contains the graph.
        """
        resolved_path = self._path_lookup.resolve(path)
        if not resolved_path:
            problem = DependencyProblem("path-not-found", "Path not found", source_path=path)
            return MaybeGraph(None, [problem])
        is_dir = resolved_path.is_dir()
        loader: DependencyLoader
        if is_a_notebook(resolved_path):
            loader = self._notebook_loader
        elif is_dir:
            loader = self._folder_loader
        else:
            loader = self._file_loader
        root_dependency = Dependency(loader, resolved_path, not is_dir)  # don't inherit context when traversing folders
        container = root_dependency.load(self._path_lookup)
        if container is None:
            problem = DependencyProblem("dependency-not-found", "Dependency not found", source_path=path)
            return MaybeGraph(None, [problem])
        session_state = self._context_factory().session_state
        graph = DependencyGraph(root_dependency, None, self._dependency_resolver, self._path_lookup, session_state)
        problems = list(container.build_dependency_graph(graph))
        if problems:
            return MaybeGraph(None, problems)
        return MaybeGraph(graph, [])
