from pathlib import Path

from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency, SourceContainer, WrappingLoader
from databricks.labs.ucx.source_code.files import FileLoader

from tests.unit import _load_sources


def test_dependency_path():
    """The dependency path should return the path."""
    path = Path("test")
    dependency = Dependency(FileLoader(), path)
    assert dependency.path == path


def test_wrapping_loader_load_dependency(mock_path_lookup):
    """Should return the source container"""
    sources = _load_sources(SourceContainer, "root4.py.txt")
    wrapping_loader = WrappingLoader(sources[0])
    dependency = create_autospec(Dependency)  # TODO: Replace autospec with object
    assert wrapping_loader.load_dependency(mock_path_lookup, dependency) == sources[0]


def test_dependency_graph_without_parent_root_is_self(mock_path_lookup):
    """The dependency graph root should be itself when there is no parent."""
    dependency = create_autospec(Dependency)
    graph = DependencyGraph(
        dependency=dependency,  # TODO: Replace autospec with object
        parent=None,
        resolver=None,  # TODO: Replace None with object
        path_lookup=mock_path_lookup
    )
    assert graph.root == graph


