from pathlib import Path

from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver, FolderLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.labs.ucx.source_code.known import Whitelist


def test_dependency_graph_registers_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    file_loader = FileLoader()
    whitelist = Whitelist()
    dependency_resolver = DependencyResolver(
        PipResolver(whitelist),
        NotebookResolver(NotebookLoader()),
        ImportFileResolver(file_loader, whitelist),
        mock_path_lookup,
    )
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)

    problems = graph.register_library("demo-egg")  # installs pkgdir

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pkgdir")).exists()


def test_folder_loads_content(mock_path_lookup):
    path = Path(Path(__file__).parent, "samples")
    file_loader = FileLoader()
    whitelist = Whitelist()
    dependency_resolver = DependencyResolver(
        PipResolver(whitelist),
        NotebookResolver(NotebookLoader()),
        ImportFileResolver(file_loader, whitelist),
        mock_path_lookup,
    )
    dependency = Dependency(FolderLoader(file_loader), path)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)
    container = dependency.load(mock_path_lookup)
    container.build_dependency_graph(graph)
    assert len(graph.all_paths) > 1
