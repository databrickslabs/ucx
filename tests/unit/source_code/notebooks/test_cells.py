from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, PipCell
from databricks.labs.ucx.source_code.notebooks.loaders import (
    NotebookResolver,
    NotebookLoader,
)
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.labs.ucx.source_code.known import Whitelist


def test_pip_cell_language_is_pip():
    assert PipCell("code").language == CellLanguage.PIP


def test_pip_cell_build_dependency_graph_invokes_register_library():
    graph = create_autospec(DependencyGraph)

    code = "%pip install databricks"
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    graph.register_library.assert_called_once_with("databricks")


def test_pip_cell_build_dependency_graph_pip_registers_missing_library():
    graph = create_autospec(DependencyGraph)

    code = "%pip install"
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Missing arguments in '%pip install'"
    graph.register_library.assert_not_called()


def test_pip_cell_build_dependency_graph_reports_incorrect_syntax():
    graph = create_autospec(DependencyGraph)

    code = "%pip installl pytest"  # typo on purpose
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Unsupported %pip command: installl"
    graph.register_library.assert_not_called()


def test_pip_cell_build_dependency_graph_reports_unknown_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(
        [PipResolver(FileLoader(), Whitelist())], notebook_resolver, [], mock_path_lookup
    )
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)

    code = "%pip install unknown-library-name"
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install unknown-library-name")


def test_pip_cell_build_dependency_graph_resolves_installed_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(
        [PipResolver(FileLoader(), Whitelist())], notebook_resolver, [], mock_path_lookup
    )
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)

    code = "%pip install demo-egg"  # installs pkgdir
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pkgdir")).exists()


def test_pip_cell_build_dependency_graph_handles_multiline_code():
    graph = create_autospec(DependencyGraph)

    code = "%pip install databricks\nmore code defined"
    cell = PipCell(code)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    graph.register_library.assert_called_once_with("databricks")
