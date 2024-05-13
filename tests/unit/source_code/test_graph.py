from pathlib import Path

import pytest
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyResolver,
    WrappingLoader,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.files import FileLoader, LocalFileResolver


def test_dependency_path():
    """The dependency path should return the path."""
    path = Path("test")
    dependency = Dependency(FileLoader(), path)
    assert dependency.path == path


@pytest.fixture()
def file_dependency() -> Dependency:
    yield Dependency(FileLoader(), Path("test"))


def test_wrapping_loader_load_dependency(mock_path_lookup, file_dependency):
    """Should return the source container"""
    source_file = mock_path_lookup.resolve(Path("root4.py.txt"))
    source = source_file.read_text()
    wrapping_loader = WrappingLoader(source)
    assert wrapping_loader.load_dependency(mock_path_lookup, file_dependency) == source


def test_dependency_graph_without_parent_root_is_self(mock_path_lookup, file_dependency):
    """The dependency graph root should be itself when there is no parent."""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )
    assert graph.root == graph


def test_dependency_graph_no_visit_when_visited(mock_path_lookup, file_dependency):
    """If node is visited, it should not be visited again."""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    assert not graph.visit(lambda _: True, {graph.path})


@pytest.mark.parametrize("visit", [True, False])
def test_dependency_graph_visit(mock_path_lookup, file_dependency, visit):
    """Visit the node, or not"""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    assert graph.visit(lambda _: visit, set()) == visit


def test_dependency_graph_locate_dependency_not_found(mock_path_lookup, file_dependency):
    """Locate a dependency that is not found"""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)

    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    maybe = graph.locate_dependency(Path("/path/to/non/existing/dependency"))
    assert len(maybe.problems) > 0
    assert maybe.problems[0] == DependencyProblem("dependency-not-found", 'Dependency not found')


def test_dependency_graph_locate_dependency_found(mock_path_lookup, file_dependency):
    """Dependency graph should be able to locate its dependency"""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)

    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    maybe = graph.locate_dependency(file_dependency.path)
    assert len(maybe.problems) == 0
    assert maybe.graph == graph


def test_dependency_graph_register_dependency_not_found(mock_path_lookup, file_dependency):
    """Register a dependency that is not found"""
    dependency_not_found = Dependency(FileLoader(), Path("/path/to/non/existing/dependency"))
    expected_dependency_problem = DependencyProblem(
        "dependency-register-failed", 'Failed to register dependency', dependency_not_found.path
    )

    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    maybe = graph.register_dependency(dependency_not_found)

    assert len(maybe.problems) > 0
    assert maybe.problems[0] == expected_dependency_problem


def test_dependency_graph_register_locatable_dependency(mock_path_lookup, file_dependency):
    """Register a locatable dependency"""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    maybe = graph.register_dependency(file_dependency)

    assert len(maybe.problems) == 0
    assert maybe.graph.dependency == file_dependency


def test_dependency_graph_register_new_dependency(mock_path_lookup, file_dependency):
    """Register a new dependency"""
    new_dependency = Dependency(FileLoader(), mock_path_lookup.resolve(Path("root1.run.py")))
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    maybe = graph.register_dependency(new_dependency)

    assert len(maybe.problems) == 0
    assert maybe.graph.dependency == new_dependency


def test_dependency_graph_register_new_dependency_with_problem(mock_path_lookup, file_dependency):
    """Register a new dependency with a problem."""
    new_dependency = Dependency(FileLoader(), mock_path_lookup.resolve(Path("root10.py")))
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency, parent=None, resolver=dependency_resolver, path_lookup=mock_path_lookup
    )

    maybe = graph.register_dependency(new_dependency)

    assert len(maybe.problems) == 1
    assert maybe.problems[0].source_path == new_dependency.path
