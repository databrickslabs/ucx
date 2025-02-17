from __future__ import annotations

import abc
import logging
import itertools
from collections.abc import Callable, Iterator, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import TypeVar, Generic

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    CurrentSessionState,
    DirectFsAccess,
    LineageAtom,
    LocatedAdvice,
    UsedTable,
    SourceInfo,
    infer_file_language_if_supported,
    is_a_notebook,
    safe_read_text,
)
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.linters.files import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import MaybeTree, Tree
from databricks.labs.ucx.source_code.linters.python import PythonSequentialLinter

logger = logging.getLogger(__name__)
T = TypeVar("T")


class DependencyGraphWalker(abc.ABC, Generic[T]):
    """Walks over the dependencies in a graph starting from the root dependencies going depth first.

    Implemented as an object to iterate over, for example:
    ``` python
    walker = DependencyGraphWalker()
    for processed_dependency_output in walker:
        # Do something with output
    ```
    """

    def __init__(self, graph: DependencyGraph, path_lookup: PathLookup):
        self._graph = graph
        self._path_lookup = path_lookup

        self._walked_paths = set[Path]()
        self._lineage = list[Dependency]()

    def __iter__(self) -> Iterator[T]:
        """Iterate over the dependencies starting from the root."""
        for dependency in self._graph.root_dependencies:
            # the dependency is a root, so its path is the one to use
            # for computing lineage and building python global context
            yield from self._iter_one(dependency, self._graph, dependency.path)

    def _iter_one(self, dependency: Dependency, graph: DependencyGraph, root_path: Path) -> Iterable[T]:
        """Iterate over a single dependency going depth first."""
        if dependency.path in self._walked_paths:
            # TODO: Decide to not skip dependencies that have been walked already.
            # Open questions:
            # - Should this come before or after the lineage logging?
            # - When do we reach this? Also, it could mean that is coming from a different root,
            #   which maybe needs to be processed.
            return
        self._lineage.append(dependency)
        self._walked_paths.add(dependency.path)
        self._log_walk_one(dependency)
        inherited_tree = graph.root.build_inherited_tree(root_path, dependency.path)
        path_lookup = self._path_lookup.change_directory(dependency.path.parent)
        yield from self._process_dependency(dependency, path_lookup, inherited_tree)
        maybe_graph = graph.locate_dependency(dependency.path)
        # missing graph problems have already been reported while building the graph
        if maybe_graph.graph:
            child_graph = maybe_graph.graph
            # This makes the implementation depth first
            for child_dependency in child_graph.local_dependencies:
                yield from self._iter_one(child_dependency, child_graph, root_path)
        self._lineage.pop()

    def _log_walk_one(self, dependency: Dependency) -> None:
        """Possibly overwrite this method in a subclass for more specific logging"""
        logger.debug(f'Analyzing dependency: {dependency}')

    @abc.abstractmethod
    def _process_dependency(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        inherited_tree: Tree | None,
    ) -> Iterable[T]:
        """Process a dependency."""

    @property
    def lineage(self) -> list[LineageAtom]:
        """The lineage for getting to the dependency."""
        lists: list[list[LineageAtom]] = [dependency.lineage for dependency in self._lineage]
        return list(itertools.chain(*lists))


class LinterWalker(DependencyGraphWalker[LocatedAdvice]):
    """Lint the dependencies in the graph."""

    def __init__(self, graph: DependencyGraph, path_lookup: PathLookup, context_factory: Callable[[], LinterContext]):
        super().__init__(graph, path_lookup)
        self._context_factory = context_factory

    def _log_walk_one(self, dependency: Dependency) -> None:
        """Log linting a dependency"""
        logger.info(f"Linting dependency: {dependency}")

    def _process_dependency(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        inherited_tree: Tree | None,
    ) -> Iterable[LocatedAdvice]:
        """Lint the dependency and yield the located advices."""
        # FileLinter determines which file/notebook linter to use
        linter = FileLinter(dependency, path_lookup, self._context_factory(), inherited_tree)
        for advice in linter.lint():
            yield LocatedAdvice(advice, dependency.path)


class FixerWalker(DependencyGraphWalker[None]):
    """Fix the dependencies in the graph."""

    def __init__(self, graph: DependencyGraph, path_lookup: PathLookup, context_factory: Callable[[], LinterContext]):
        super().__init__(graph, path_lookup)
        self._context_factory = context_factory

    def _log_walk_one(self, dependency: Dependency) -> None:
        """Log fixing a dependency"""
        logger.info(f"Fixing dependency: {dependency}")

    def _process_dependency(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        inherited_tree: Tree | None,
    ) -> Iterable[None]:
        """Fix the dependency."""
        # FileLinter determines which file/notebook linter to use
        linter = FileLinter(dependency, path_lookup, self._context_factory(), inherited_tree)
        linter.apply()
        yield from ()


S = TypeVar("S", bound=SourceInfo)


class _CollectorWalker(DependencyGraphWalker[S], abc.ABC):

    def __init__(
        self,
        graph: DependencyGraph,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        migration_index: TableMigrationIndex,
    ):
        super().__init__(graph, path_lookup)
        self._linter_context = LinterContext(migration_index, session_state)

    def _process_dependency(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        inherited_tree: Tree | None,
    ) -> Iterable[S]:
        language = infer_file_language_if_supported(dependency.path)
        if not language:
            logger.warning(f"Unsupported language for {dependency.path}")
            return
        cell_language = CellLanguage.of_language(language)
        source = safe_read_text(dependency.path)
        if not source:
            return
        if is_a_notebook(dependency.path):
            yield from self._collect_from_notebook(source, cell_language, dependency.path, inherited_tree)
        elif dependency.path.is_file():
            yield from self._collect_from_source(source, cell_language, dependency.path, inherited_tree)

    def _collect_from_notebook(
        self,
        source: str,
        language: CellLanguage,
        path: Path,
        inherited_tree: Tree | None,
    ) -> Iterable[S]:
        notebook = Notebook.parse(path, source, language.language)
        src_timestamp = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        src_id = str(path)
        for cell in notebook.cells:
            for item in self._collect_from_source(cell.original_code, cell.language, path, inherited_tree):
                yield item.replace_source(source_id=src_id, source_lineage=self.lineage, source_timestamp=src_timestamp)
            if cell.language is CellLanguage.PYTHON:
                if inherited_tree is None:
                    inherited_tree = Tree.new_module()
                maybe_tree = MaybeTree.from_source_code(cell.original_code)
                if maybe_tree.failure:
                    logger.warning(maybe_tree.failure.message)
                    continue
                assert maybe_tree.tree is not None
                inherited_tree.attach_child_tree(maybe_tree.tree)

    def _collect_from_source(
        self,
        source: str,
        language: CellLanguage,
        path: Path,
        inherited_tree: Tree | None,
    ) -> Iterable[S]:
        if language is CellLanguage.PYTHON:
            iterable = self._collect_from_python(source, inherited_tree)
        else:
            fn: Callable[[str], Iterable[S]] | None = getattr(self, f"_collect_from_{language.name.lower()}", None)
            if not fn:
                raise ValueError(f"Language {language.name} not supported yet!")
            # the below is for disabling a false pylint positive
            # pylint: disable=not-callable
            iterable = fn(source)
        src_timestamp = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        src_id = str(path)
        for item in iterable:
            yield item.replace_source(source_id=src_id, source_lineage=self.lineage, source_timestamp=src_timestamp)

    @abc.abstractmethod
    def _collect_from_python(self, source: str, inherited_tree: Tree | None) -> Iterable[S]: ...

    def _collect_from_sql(self, _source: str) -> Iterable[S]:
        return []

    def _collect_from_r(self, _source: str) -> Iterable[S]:
        logger.warning("Language R not supported yet!")
        return []

    def _collect_from_scala(self, _source: str) -> Iterable[S]:
        logger.warning("Language scala not supported yet!")
        return []

    def _collect_from_shell(self, _source: str) -> Iterable[S]:
        return []

    def _collect_from_markdown(self, _source: str) -> Iterable[S]:
        return []

    def _collect_from_run(self, _source: str) -> Iterable[S]:
        return []

    def _collect_from_pip(self, _source: str) -> Iterable[S]:
        return []


class DfsaCollectorWalker(_CollectorWalker[DirectFsAccess]):

    def _collect_from_python(self, source: str, inherited_tree: Tree | None) -> Iterable[DirectFsAccess]:
        collector = self._linter_context.dfsa_collector(Language.PYTHON)
        yield from collector.collect_dfsas(source)

    def _collect_from_sql(self, source: str) -> Iterable[DirectFsAccess]:
        collector = self._linter_context.dfsa_collector(Language.SQL)
        yield from collector.collect_dfsas(source)


class TablesCollectorWalker(_CollectorWalker[UsedTable]):

    def _collect_from_python(self, source: str, inherited_tree: Tree | None) -> Iterable[UsedTable]:
        collector = self._linter_context.tables_collector(Language.PYTHON)
        assert isinstance(collector, PythonSequentialLinter)
        yield from collector.collect_tables(source)

    def _collect_from_sql(self, source: str) -> Iterable[UsedTable]:
        collector = self._linter_context.tables_collector(Language.SQL)
        yield from collector.collect_tables(source)
