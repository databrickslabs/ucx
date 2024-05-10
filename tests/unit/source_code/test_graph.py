from pathlib import Path

import pytest
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver, SourceContainer, WrappingLoader
from databricks.labs.ucx.source_code.files import FileLoader, LocalFileResolver

from tests.unit import _load_sources


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
    sources = _load_sources(SourceContainer, "root4.py.txt")
    wrapping_loader = WrappingLoader(sources[0])
    assert wrapping_loader.load_dependency(mock_path_lookup, file_dependency) == sources[0]


def test_dependency_graph_without_parent_root_is_self(mock_path_lookup, file_dependency):
    """The dependency graph root should be itself when there is no parent."""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency,
        parent=None,
        resolver=dependency_resolver,
        path_lookup=mock_path_lookup
    )
    assert graph.root == graph


def test_dependency_graph_no_visit_when_visited(mock_path_lookup, file_dependency):
    """If node is visited, it should not be visited again."""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency,
        parent=None,
        resolver=dependency_resolver,
        path_lookup=mock_path_lookup
    )

    assert not graph.visit(lambda _: True, {graph.path})


@pytest.mark.parametrize("visit", [True, False])
def test_dependency_graph_visit(mock_path_lookup, file_dependency, visit):
    """Visit the node, or not"""
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], mock_path_lookup)
    graph = DependencyGraph(
        dependency=file_dependency,
        parent=None,
        resolver=dependency_resolver,
        path_lookup=mock_path_lookup
    )

    assert graph.visit(lambda _: visit, set()) == visit
