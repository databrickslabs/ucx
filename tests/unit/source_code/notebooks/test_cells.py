from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.graph import DependencyGraph
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, PipCell


def test_pip_cell_langauge():
    """Pip cell langauge should be CellLanguage.PIP"""
    assert PipCell("code").language == CellLanguage.PIP


def test_pip_cell_build_dependency_graph_register_pip_install_library():
    """A pip install library should be registered"""
    graph = create_autospec(DependencyGraph)

    code = "%pip install databricks"
    cell = PipCell(code)

    cell.build_dependency_graph(graph)

    graph.register_library.assert_called_once_with("databricks")
