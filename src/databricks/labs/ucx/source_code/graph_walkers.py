from __future__ import annotations

import abc
import itertools
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Generic, TypeVar

from databricks.labs.ucx.source_code.base import is_a_notebook, LineageAtom, LocatedAdvice
from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency, logger
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import Tree


T = TypeVar("T")


class DependencyGraphWalker(abc.ABC, Generic[T]):

    def __init__(self, graph: DependencyGraph, path_lookup: PathLookup):
        self._graph = graph
        self._path_lookup = path_lookup

        self._walked_paths = set[Path]()
        self._lineage = list[Dependency]()

    def __iter__(self) -> Iterator[T]:
        for dependency in self._graph.root_dependencies:
            # the dependency is a root, so its path is the one to use
            # for computing lineage and building python global context
            root_path = dependency.path
            yield from self._iter_one(dependency, self._graph, root_path)

    def _iter_one(self, dependency: Dependency, graph: DependencyGraph, root_path: Path) -> Iterable[T]:
        if dependency.path in self._walked_paths:
            return
        self._lineage.append(dependency)
        self._walked_paths.add(dependency.path)
        self._log_walk_one(dependency)
        if dependency.path.is_file() or is_a_notebook(dependency.path):
            inherited_tree = graph.root.build_inherited_tree(root_path, dependency.path)
            path_lookup = self._path_lookup.change_directory(dependency.path.parent)
            yield from self._process_dependency(dependency, path_lookup, inherited_tree)
            maybe_graph = graph.locate_dependency(dependency.path)
            # missing graph problems have already been reported while building the graph
            if maybe_graph.graph:
                child_graph = maybe_graph.graph
                for child_dependency in child_graph.local_dependencies:
                    yield from self._iter_one(child_dependency, child_graph, root_path)
        self._lineage.pop()

    def _log_walk_one(self, dependency: Dependency) -> None:
        logger.debug(f'Analyzing dependency: {dependency}')

    @abc.abstractmethod
    def _process_dependency(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        inherited_tree: Tree | None,
    ) -> Iterable[T]: ...

    @property
    def lineage(self) -> list[LineageAtom]:
        lists: list[list[LineageAtom]] = [dependency.lineage for dependency in self._lineage]
        return list(itertools.chain(*lists))


class LintingWalker(DependencyGraphWalker[LocatedAdvice]):  # TODO : Use this and introduce a Fixer walker

    def __init__(self, graph: DependencyGraph, path_lookup: PathLookup, context_factory: Callable[[], LinterContext]):
        super().__init__(graph, path_lookup)
        self._context_factory = context_factory

    def _process_dependency(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        inherited_tree: Tree | None,
    ) -> Iterable[LocatedAdvice]:
        # FileLinter determines which file/notebook linter to use
        linter_context = self._context_factory()
        linter = FileLinter(linter_context, path_lookup, dependency.path, inherited_tree)
        for advice in linter.lint():
            yield LocatedAdvice(advice, dependency.path)
