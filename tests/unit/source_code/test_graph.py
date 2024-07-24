from pathlib import Path

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
    dependency = Dependency(FolderLoader(file_loader), path)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup, session_state)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(graph)
    assert len(graph.all_paths) > 1


def test_graph_computes_route(mock_path_lookup, simple_dependency_resolver):
    grand_parent = mock_path_lookup.cwd / "functional/grand_parent_that_runs_parent_that_runs_child.py"
    parent = mock_path_lookup.cwd / "functional/parent_that_runs_child_that_uses_value_from_parent.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(FileLoader(), grand_parent)

    class TestDependencyGraph(DependencyGraph):
        def compute_route(self, root: Path, leaf: Path) -> list[Dependency]:
            return self._compute_route(root, leaf, set())

    root_graph = TestDependencyGraph(
        dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState()
    )
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    route = root_graph.compute_route(grand_parent, child)
    assert [dep.path for dep in route] == [grand_parent, parent]


def test_graph_builds_inherited_context(mock_path_lookup, simple_dependency_resolver):
    parent = mock_path_lookup.cwd / "functional/grand_parent_that_runs_parent_that_runs_child.py"
    child = mock_path_lookup.cwd / "functional/_child_that_uses_value_from_parent.py"
    dependency = Dependency(FileLoader(), parent)
    root_graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(root_graph)
    inference_context = root_graph.build_inherited_context(parent, child)
    assert inference_context.found is True
    assert inference_context.tree is not None
    assert inference_context.tree.has_global("some_table_name")
    assert not inference_context.tree.has_global("other_table_name")
