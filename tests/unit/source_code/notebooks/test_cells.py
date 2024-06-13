from pathlib import Path
from unittest.mock import create_autospec
import yaml

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, PipCell
from databricks.labs.ucx.source_code.notebooks.loaders import (
    NotebookResolver,
    NotebookLoader,
)
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import Whitelist


def test_basic_cell_extraction() -> None:
    """Ensure that simple cell splitting (including cell type) and offset tracking works as intended."""
    # Fixture: a basic notebook with a few cell types, along with a YAML-encoded description of the cells.
    sample_notebook_path = Path(__file__).parent.parent / "samples" / "simple_notebook.py"
    with sample_notebook_path.open() as f:
        sample_notebook_content = f.read()
    cell_description_path = sample_notebook_path.parent / "simple_notebook.yml"
    with cell_description_path.open() as f:
        cell_descriptions = yaml.safe_load(f)

    # Perform the test.
    cells = CellLanguage.PYTHON.extract_cells(sample_notebook_content)

    # Verify the results.
    cell_metadata = cell_descriptions["cells"]
    assert len(cells) == len(cell_metadata), "Wrong number of cells"
    for i, (actual, expected) in enumerate(zip(cells, cell_metadata, strict=True)):
        assert type(actual).__name__ == f"{expected['type']}Cell", f"Cell {i} is of the wrong type"
        assert actual.original_offset == expected["starts_at_line"], f"Cell {i} starts on the wrong line"
        # TODO: Fix content checking. Current problems:
        #  - Chomping of the final line ending.
        #  - Various MAGIC/COMMENT/etc prefixes seem to end up in the content.
        # asssert actual.original_code == expected["content"], f"Cell {i} starts on the wrong line"


def test_pip_cell_language_is_pip():
    assert PipCell("code", original_offset=1).language == CellLanguage.PIP


def test_pip_cell_build_dependency_graph_invokes_register_library():
    graph = create_autospec(DependencyGraph)

    code = "%pip install databricks"
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    graph.register_library.assert_called_once_with("databricks")


def test_pip_cell_build_dependency_graph_pip_registers_missing_library():
    graph = create_autospec(DependencyGraph)

    code = "%pip install"
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Missing arguments after '%pip install'"
    graph.register_library.assert_not_called()


def test_pip_cell_build_dependency_graph_reports_incorrect_syntax():
    graph = create_autospec(DependencyGraph)

    code = "%pip installl pytest"  # typo on purpose
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert "installl" in problems[0].message  # Message coming directly from pip
    graph.register_library.assert_not_called()


def test_pip_cell_build_dependency_graph_reports_unsupported_command():
    graph = create_autospec(DependencyGraph)

    code = "%pip freeze"
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Unsupported %pip command: freeze"
    graph.register_library.assert_not_called()


def test_pip_cell_build_dependency_graph_reports_missing_command():
    graph = create_autospec(DependencyGraph)

    code = "%pip"
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message == "Missing command after '%pip'"
    graph.register_library.assert_not_called()


def test_pip_cell_build_dependency_graph_reports_unknown_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    pip_resolver = PythonLibraryResolver(Whitelist())
    dependency_resolver = DependencyResolver(pip_resolver, notebook_resolver, [], mock_path_lookup)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup, CurrentSessionState())

    code = "%pip install unknown-library-name"
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("'pip install unknown-library-name")


def test_pip_cell_build_dependency_graph_resolves_installed_library(mock_path_lookup):
    dependency = Dependency(FileLoader(), Path("test"))
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    whitelist = Whitelist()
    file_loader = FileLoader()
    pip_resolver = PythonLibraryResolver(whitelist)
    import_resolver = ImportFileResolver(file_loader, whitelist)
    dependency_resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, mock_path_lookup)
    graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup, CurrentSessionState())

    whl = Path(__file__).parent / '../samples/distribution/dist/thingy-0.0.1-py2.py3-none-any.whl'

    code = f"%pip install {whl.as_posix()}"  # installs thingy
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("thingy")).exists()


def test_pip_cell_build_dependency_graph_handles_multiline_code():
    graph = create_autospec(DependencyGraph)

    code = "%pip install databricks\nmore code defined"
    cell = PipCell(code, original_offset=1)

    problems = cell.build_dependency_graph(graph)

    assert len(problems) == 0
    graph.register_library.assert_called_once_with("databricks")


@pytest.mark.parametrize(
    "code,split",
    [
        ("%pip install foo", ["%pip", "install", "foo"]),
        ("%pip install", ["%pip", "install"]),
        ("%pip installl foo", ["%pip", "installl", "foo"]),
        ("%pip install foo --index-url bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url \\\n bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url bar\nmore code", ["%pip", "install", "foo", "--index-url", "bar"]),
        (
            "%pip install foo --index-url bar\\\n -t /tmp/",
            ["%pip", "install", "foo", "--index-url", "bar", "-t", "/tmp/"],
        ),
        ("%pip install foo --index-url \\\n bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        (
            "%pip install ./distribution/dist/thingy-0.0.1-py2.py3-none-any.whl",
            ["%pip", "install", "./distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"],
        ),
        (
            "%pip install distribution/dist/thingy-0.0.1-py2.py3-none-any.whl",
            ["%pip", "install", "distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"],
        ),
    ],
)
def test_pip_cell_split(code, split):
    assert PipCell._split(code) == split  # pylint: disable=protected-access
