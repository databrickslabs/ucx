from pathlib import Path

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver, StubLibraryResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.python_libraries import PipResolver


def test_dependency_graph_registers_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    dependency_resolver = DependencyResolver([PipResolver()], NotebookResolver(NotebookLoader()), [], mock_path_lookup)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)

    problems = graph.register_library("pytest")

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pytest")).exists()


def test_stub_import_resolver_fails_with_library_not_found_dependency_problem(mock_path_lookup):
    resolver = StubLibraryResolver()
    maybe = resolver.resolve_library(mock_path_lookup, "test")

    assert len(maybe.problems) == 1
    assert maybe.problems[0].code == "library-not-found"
