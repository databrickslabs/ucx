from pathlib import Path

import pytest

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


@pytest.fixture
def graph_parent_child_context(mock_path_lookup, simple_dependency_resolver) -> DependencyGraph:
    """Dependency graph for the parent-child-context sample directory.

    The directory contains three files `grand_parent.py`, `parent.py` and `child.py`. That import each other as such:
    grand parent imports parent import child.
    """
    path = mock_path_lookup.resolve(Path("parent-child-context/"))
    dependency = Dependency(FolderLoader(NotebookLoader(), FileLoader()), path, False)
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    return graph


def test_folder_build_dependency_graph_without_problems(mock_path_lookup, graph_parent_child_context) -> None:
    """No problems should arise form building the dependency graph for the sample folder """
    folder = graph_parent_child_context.dependency.load(mock_path_lookup)
    assert folder is not None
    problems = folder.build_dependency_graph(graph_parent_child_context)
    assert not problems


def test_folder_loads_content(mock_path_lookup, graph_parent_child_context) -> None:
    """The files in the folder should be added to the dependency graph after building."""
    expected_dependencies = {graph_parent_child_context.dependency}
    for relative_path in "grand_parent.py", "parent.py", "child.py":
        path = mock_path_lookup.resolve(Path("parent-child-context") / relative_path)
        dependency = Dependency(FileLoader(), path)
        expected_dependencies.add(dependency)

    folder = graph_parent_child_context.dependency.load(mock_path_lookup)
    assert folder is not None
    folder.build_dependency_graph(graph_parent_child_context)

    assert graph_parent_child_context.all_dependencies == expected_dependencies
