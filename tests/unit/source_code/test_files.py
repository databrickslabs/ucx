from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.files import KnownContainer, KnownDependency, KnownLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup


@pytest.mark.parametrize("problems", [[], [DependencyProblem("test", "test")]])
def test_known_container_loads_problems_during_dependency_graph_building(
    simple_dependency_resolver, problems: list[DependencyProblem]
) -> None:
    path_lookup = create_autospec(PathLookup)
    dependency = KnownDependency("test", problems)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, path_lookup, CurrentSessionState())
    container = KnownContainer(Path("test.py"), problems)
    assert container.build_dependency_graph(graph) == problems
    path_lookup.assert_not_called()


@pytest.mark.parametrize("problems", [[], [DependencyProblem("test", "test")]])
def test_known_loader_loads_known_container_with_problems(
    simple_dependency_resolver, problems: list[DependencyProblem]
) -> None:
    path_lookup = create_autospec(PathLookup)
    loader = KnownLoader()
    dependency = KnownDependency("test", problems)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, path_lookup, CurrentSessionState())
    container = loader.load_dependency(path_lookup, dependency)
    assert container.build_dependency_graph(graph) == problems
    path_lookup.resolve.assert_not_called()


@pytest.mark.parametrize("problems", [[], [DependencyProblem("test", "test")]])
def test_known_dependency_has_problems(problems: list[DependencyProblem]) -> None:
    dependency = KnownDependency("test", problems)
    assert dependency.problems == problems
