from pathlib import Path

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.folders import FolderLoader, Folder
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader


def test_folder_has_repr() -> None:
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    folder = Folder(Path("test"), notebook_loader, file_loader, FolderLoader(notebook_loader, file_loader))
    assert len(repr(folder)) > 0


def test_folder_loads_content(mock_path_lookup, simple_dependency_resolver) -> None:
    expected_dependencies = set()
    for relative_path in "grand_parent.py", "parent.py", "child.py":
        path = mock_path_lookup.resolve(Path("parent-child-context") / relative_path)
        dependency = Dependency(FileLoader(), path)
        expected_dependencies.add(dependency)

    path = mock_path_lookup.resolve(Path("parent-child-context/"))
    dependency = Dependency(FolderLoader(NotebookLoader(), FileLoader()), path, False)
    expected_dependencies.add(dependency)
    container = dependency.load(mock_path_lookup)
    assert container is not None
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    container.build_dependency_graph(graph)

    assert graph.all_dependencies == expected_dependencies
