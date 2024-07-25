from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver, FolderLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import KnownList


def test_dependency_graph_registers_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    file_loader = FileLoader()
    allow_list = KnownList()
    session_state = CurrentSessionState()
    dependency_resolver = DependencyResolver(
        PythonLibraryResolver(allow_list),
        NotebookResolver(NotebookLoader()),
        ImportFileResolver(file_loader, allow_list),
        mock_path_lookup,
    )
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup, session_state)

    problems = graph.register_library("demo-egg")  # installs pkgdir

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pkgdir")).exists()


def test_folder_loads_content(mock_path_lookup):
    path = Path(Path(__file__).parent, "samples")
    file_loader = FileLoader()
    allow_list = KnownList()
    session_state = CurrentSessionState()
    dependency_resolver = DependencyResolver(
        PythonLibraryResolver(allow_list),
        NotebookResolver(NotebookLoader()),
        ImportFileResolver(file_loader, allow_list),
        mock_path_lookup,
    )
    dependency = Dependency(FolderLoader(file_loader), path, False)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup, session_state)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(graph)
    assert len(graph.all_paths) > 1


class _TestDependencyGraph(DependencyGraph):
    def compute_route(self, root: Path, leaf: Path) -> list[Dependency]:
        return self._compute_route(root, leaf, set())


@pytest.fixture()
def dependency_graph_factory(mock_path_lookup, simple_dependency_resolver):

    def new_test_dependency_graph(dependency: Dependency) -> _TestDependencyGraph:
        return _TestDependencyGraph(
            dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
        )

    return new_test_dependency_graph


def test_graph_computes_magic_run_route(mock_path_lookup, dependency_graph_factory):
    parent = mock_path_lookup.cwd / "functional/parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(parent, child)
    assert [dep.path for dep in route] == [parent, child]


def test_graph_computes_magic_run_route_recursively(mock_path_lookup, dependency_graph_factory):
    grand_parent = mock_path_lookup.cwd / "functional/grand_parent_that_magic_runs_parent_that_magic_runs_child.py"
    parent = mock_path_lookup.cwd / "functional/parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [grand_parent, parent, child]


def test_graph_computes_dbutils_run_route(mock_path_lookup, dependency_graph_factory):
    parent = mock_path_lookup.cwd / "functional/parent_that_dbutils_runs_child_that_misses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_missing_value.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(parent, child)
    assert [dep.path for dep in route] == [child]


def test_graph_computes_dbutils_run_route_recursively(mock_path_lookup, dependency_graph_factory):
    grand_parent = mock_path_lookup.cwd / "functional/grand_parent_that_dbutils_runs_parent_that_magic_runs_child.py"
    parent = mock_path_lookup.cwd / "functional/parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [parent, child]


def test_graph_computes_import_route(mock_path_lookup, dependency_graph_factory):
    parent = mock_path_lookup.cwd / "functional/parent_that_imports_child_that_misses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_missing_value.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(parent, child)
    assert [dep.path for dep in route] == [child]


def test_graph_computes_import_route_recursively(mock_path_lookup, dependency_graph_factory):
    grand_parent = mock_path_lookup.cwd / "functional/grand_parent_that_imports_parent_that_magic_runs_child.py"
    parent = mock_path_lookup.cwd / "functional/parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [parent, child]
