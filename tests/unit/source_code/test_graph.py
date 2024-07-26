from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver, FolderLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver, InheritedContext
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


def test_folder_loads_content(mock_path_lookup, simple_dependency_resolver):
    path = Path(__file__).parent / "samples"
    dependency = Dependency(FolderLoader(FileLoader()), path, False)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(graph)
    assert len(graph.all_paths) > 1


def test_root_dependencies_returns_only_files(mock_path_lookup, simple_dependency_resolver):
    path = Path(__file__).parent / "samples" / "parent-child-context"
    dependency = Dependency(FolderLoader(FileLoader()), path, False)
    container = dependency.load(mock_path_lookup)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    container.build_dependency_graph(graph)
    roots = graph.root_dependencies
    actual = list(root.path for root in roots)
    assert actual == [path / "grand_parent.py"]


class _TestDependencyGraph(DependencyGraph):
    def compute_route(self, root: Path, leaf: Path) -> list[Dependency]:
        return self._compute_route(root, leaf, set())

    def build_inherited_context(self, root: Path, leaf: Path) -> InheritedContext:
        return self._build_inherited_context(root, leaf)


@pytest.fixture()
def dependency_graph_factory(mock_path_lookup, simple_dependency_resolver):

    def new_test_dependency_graph(dependency: Dependency) -> _TestDependencyGraph:
        return _TestDependencyGraph(
            dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
        )

    return new_test_dependency_graph


def test_graph_computes_magic_run_route(mock_path_lookup, dependency_graph_factory):
    parent = mock_path_lookup.cwd / "functional" / "parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional" / "_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(parent, child)
    assert [dep.path for dep in route] == [parent, child]


def test_graph_computes_magic_run_route_recursively(mock_path_lookup, dependency_graph_factory):
    grand_parent = mock_path_lookup.cwd / "functional" / "grand_parent_that_magic_runs_parent_that_magic_runs_child.py"
    parent = mock_path_lookup.cwd / "functional" / "parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional" / "_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [grand_parent, parent, child]


def test_graph_computes_magic_run_route_recursively_in_parent_folder(mock_path_lookup, dependency_graph_factory):
    parent_folder = mock_path_lookup.cwd / "parent-child-context"
    grand_parent = parent_folder / "grand_parent.py"
    parent = parent_folder / "parent.py"
    child = parent_folder / "child.py"
    dependency = Dependency(FolderLoader(FileLoader()), parent_folder)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    roots = root_graph.root_dependencies
    assert len(roots) == 1
    assert grand_parent in [dep.path for dep in roots]
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [grand_parent, parent, child]


def test_graph_computes_dbutils_run_route(mock_path_lookup, dependency_graph_factory):
    parent = mock_path_lookup.cwd / "functional" / "parent_that_dbutils_runs_child_that_misses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional" / "_child_that_uses_missing_value.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(parent, child)
    assert [dep.path for dep in route] == [child]


def test_graph_computes_dbutils_run_route_recursively(mock_path_lookup, dependency_graph_factory):
    grand_parent = (
        mock_path_lookup.cwd / "functional" / "grand_parent_that_dbutils_runs_parent_that_magic_runs_child.py"
    )
    parent = mock_path_lookup.cwd / "functional" / "parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional" / "_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [parent, child]


def test_graph_computes_import_route(mock_path_lookup, dependency_graph_factory):
    parent = mock_path_lookup.cwd / "functional" / "parent_that_imports_child_that_misses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional" / "_child_that_uses_missing_value.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(parent, child)
    assert [dep.path for dep in route] == [child]


def test_graph_computes_import_route_recursively(mock_path_lookup, dependency_graph_factory):
    grand_parent = mock_path_lookup.cwd / "functional" / "grand_parent_that_imports_parent_that_magic_runs_child.py"
    parent = mock_path_lookup.cwd / "functional" / "parent_that_magic_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional" / "_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), grand_parent)
    root_graph = dependency_graph_factory(dependency)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [parent, child]


def test_graph_builds_inherited_context(mock_path_lookup, simple_dependency_resolver):
    parent = mock_path_lookup.cwd / "functional/grand_parent_that_magic_runs_parent_that_magic_runs_child.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(NotebookLoader(), parent)
    root_graph = _TestDependencyGraph(
        dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
    )
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    inference_context = root_graph.build_inherited_context(parent, child)
    assert inference_context.found is True
    assert inference_context.tree is not None
    assert inference_context.tree.has_global("some_table_name")
    assert not inference_context.tree.has_global("other_table_name")
