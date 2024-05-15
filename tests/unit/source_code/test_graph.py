from pathlib import Path

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.site_packages import PipResolver


def test_dependency_graph_register_library(mock_path_lookup):
    """Install pytest using pip installer"""
    dependency = Dependency(FileLoader(), Path("test"))
    dependency_resolver = DependencyResolver([PipResolver()], mock_path_lookup)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)

    problems = graph.register_library("pytest")

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pytest")).exists()
