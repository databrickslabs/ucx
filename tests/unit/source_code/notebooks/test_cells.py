from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver, LibraryInstaller
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, PipCell
from databricks.labs.ucx.source_code.site_packages import PipInstaller


def test_pip_cell_langauge():
    """Pip cell langauge should be CellLanguage.PIP"""
    assert PipCell("code").language == CellLanguage.PIP


def test_pip_cell_build_dependency_graph_register_pip_install_library():
    """A pip install library should be registered"""
    graph = create_autospec(DependencyGraph)

    code = "%pip install databricks"
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    graph.install_library.assert_called_once_with("databricks")


def test_pip_cell_build_dependency_graph_register_pip_install_missing_library():
    """Missing pip install library"""
    graph = create_autospec(DependencyGraph)

    code = "%pip install"
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Missing arguments in '%pip install'"
    graph.install_library.assert_not_called()


def test_pip_cell_build_dependency_graph_register_pip_missing_install():
    """Missing pip install"""
    graph = create_autospec(DependencyGraph)

    code = "%pip installl pytest"  # typo on purpose
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Missing install in '%pip installl pytest'"
    graph.install_library.assert_not_called()


def test_pip_cell_build_dependency_graph_register_pip_install_unknown_library(mock_path_lookup):
    """Pip install unknown library"""
    dependency = Dependency(FileLoader(), Path("test"))
    installer = LibraryInstaller([PipInstaller()])
    dependency_resolver = DependencyResolver([], mock_path_lookup)
    graph = DependencyGraph(dependency, None, installer, dependency_resolver, mock_path_lookup)

    code = "%pip install unknown-library-name"  # typo on purpose
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install unknown-library-name")
