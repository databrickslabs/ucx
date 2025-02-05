from collections.abc import Iterable
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState, DirectFsAccess
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.graph_walkers import (
    DependencyGraphWalker,
    LintingWalker,
    DfsaCollectorWalker,
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import Tree


def test_graph_walker_captures_lineage(mock_path_lookup, simple_dependency_resolver) -> None:
    grand_parent = mock_path_lookup.cwd / "functional/grand_parent_that_magic_runs_parent_that_magic_runs_child.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    root_dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = DependencyGraph(
        root_dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
    )
    container = root_dependency.load(mock_path_lookup)
    assert container is not None
    container.build_dependency_graph(root_graph)

    class _TestWalker(DependencyGraphWalker):
        def _process_dependency(
            self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
        ) -> Iterable[None]:
            if dependency.path.as_posix().endswith(grand_parent.as_posix()):
                assert len(self._lineage) == 1
            if dependency.path.as_posix().endswith(child.as_posix()):
                assert len(self._lineage) == 3  # there's a parent between grand_parent and child
            return []

    walker = _TestWalker(root_graph, mock_path_lookup)
    list(walker)


def test_linting_walker_populates_paths(simple_dependency_resolver, mock_path_lookup, migration_index) -> None:
    path = mock_path_lookup.resolve(Path("functional/values_across_notebooks_dbutils_notebook_run.py"))
    root = Dependency(NotebookLoader(), path)
    xgraph = DependencyGraph(root, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    current_session = CurrentSessionState()
    walker = LintingWalker(
        xgraph, set(), mock_path_lookup, "key", lambda: LinterContext(migration_index, current_session)
    )
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
