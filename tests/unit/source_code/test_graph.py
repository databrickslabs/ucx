from pathlib import Path

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.site_packages import PipInstaller


def test_dependency_graph_install_library_pytest(mock_path_lookup):
    """Install pytest using pip installer"""
    dependency = Dependency(FileLoader(), Path("test"))
    installer = PipInstaller()
    dependency_resolver = DependencyResolver([], mock_path_lookup)
    graph = DependencyGraph(dependency, None, installer, dependency_resolver, mock_path_lookup)

    dependency_problems = graph.install_library("pytest")

    assert len(dependency_problems) == 0
    assert graph.path_lookup.resolve(Path("pytest")).exists()
