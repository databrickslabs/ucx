from pathlib import Path

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver, FolderLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import AllowList


def test_dependency_graph_registers_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    file_loader = FileLoader()
    allow_list = AllowList()
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
    allow_list = AllowList()
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
