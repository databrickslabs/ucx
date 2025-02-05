import logging
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState, DirectFsAccess
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.graph_walkers import (
    DependencyGraphWalker,
    DfsaCollectorWalker,
    LintingWalker,
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import Tree


@pytest.fixture
def grand_parent_graph(simple_dependency_resolver, mock_path_lookup) -> DependencyGraph:
    path = mock_path_lookup.resolve(Path("parent-child-context/grand_parent.py"))
    dependency = Dependency(NotebookLoader(), path)
    current_session = CurrentSessionState()
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, current_session)
    container = graph.dependency.load(graph.path_lookup)
    assert container is not None
    container.build_dependency_graph(graph)
    return graph


def test_graph_walker_captures_lineage(mock_path_lookup, grand_parent_graph: DependencyGraph) -> None:
    path = mock_path_lookup.resolve(Path("parent-child-context/child.py"))
    child_dependency = Dependency(NotebookLoader(), path)

    class _TestWalker(DependencyGraphWalker):
        def _process_dependency(
            self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
        ) -> Iterable[None]:
            if dependency == grand_parent_graph.dependency:
                assert len(self._lineage) == 1
            elif dependency == child_dependency:
                assert len(self._lineage) == 3  # there's a parent between grand_parent and child
            return []

    walker = _TestWalker(grand_parent_graph, mock_path_lookup)
    list(walker)


def test_graph_walker_captures_walked_paths(mock_path_lookup, grand_parent_graph: DependencyGraph) -> None:
    path = mock_path_lookup.resolve(Path("parent-child-context/child.py"))
    child_dependency = Dependency(NotebookLoader(), path)

    class _TestWalker(DependencyGraphWalker):
        walked_paths_count = 1

        def _process_dependency(
            self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
        ) -> Iterable[None]:
            assert len(self._walked_paths) == self.walked_paths_count
            self.walked_paths_count += 1
            if dependency == grand_parent_graph.dependency:
                assert grand_parent_graph.dependency.path in self._walked_paths
            elif dependency == child_dependency:
                assert grand_parent_graph.dependency.path in self._walked_paths
                assert child_dependency.path in self._walked_paths
            return []

    walker = _TestWalker(grand_parent_graph, mock_path_lookup)
    list(walker)


def test_graph_walker_logs_analyzing_dependency_in_debug(
    caplog, mock_path_lookup, grand_parent_graph: DependencyGraph
) -> None:

    class _TestWalker(DependencyGraphWalker):

        def _process_dependency(
            self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
        ) -> Iterable[None]:
            return []

    walker = _TestWalker(grand_parent_graph, mock_path_lookup)
    with caplog.at_level(logging.DEBUG, logger="databricks.labs.ucx.source_code.linters.graph_walkers"):
        list(walker)
    assert f"Analyzing dependency: {grand_parent_graph.dependency}" in caplog.messages


def test_linting_walker_populates_paths(simple_dependency_resolver, mock_path_lookup, migration_index) -> None:
    path = mock_path_lookup.resolve(Path("functional/values_across_notebooks_dbutils_notebook_run.py"))
    root = Dependency(NotebookLoader(), path)
    xgraph = DependencyGraph(root, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    current_session = CurrentSessionState()
    walker = LintingWalker(xgraph, mock_path_lookup, lambda: LinterContext(migration_index, current_session))
    advices = 0
    for advice in walker:
        advices += 1
        assert "UNKNOWN" not in advice.path.as_posix()
    assert advices


class _TestCollectorWalker(DfsaCollectorWalker):
    # inherit from DfsaCollectorWalker because it's public

    def collect_from_source(self, language: CellLanguage) -> Iterable[DirectFsAccess]:
        return self._collect_from_source("empty", language, Path(""), None)


@pytest.mark.parametrize("language", list(iter(CellLanguage)))
def test_collector_supports_all_cell_languages(language, mock_path_lookup, migration_index):
    graph = create_autospec(DependencyGraph)
    graph.assert_not_called()
    collector = _TestCollectorWalker(graph, set(), mock_path_lookup, CurrentSessionState(), migration_index)
    list(collector.collect_from_source(language))
