import logging
from collections.abc import Iterable

from astroid import NodeNG, Module  # type: ignore

from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    DirectFsAccessNode,
    DirectFsAccess,
    UsedTableNode,
    UsedTable,
)
from databricks.labs.ucx.source_code.linters.python import (
    PythonSequentialLinter,
)
from databricks.labs.ucx.source_code.linters.base import PythonLinter, DfsaPyCollector, TablePyCollector
from databricks.labs.ucx.source_code.python.python_ast import Tree


def test_dummy_python_linter_lint_lints_tree() -> None:
    linter = DummyPythonLinter()
    advices = list(linter.lint("print(1)"))
    assert advices == [Advice("dummy", "dummy advice", 0, 0, 0, 0), Advice("dummy", "dummy advice", 1, 1, 1, 1)]


def test_dummy_python_linter_lint_yields_failure_due_to_parse_error() -> None:
    linter = DummyPythonLinter()
    advices = list(linter.lint("print(1"))  # Closing parenthesis is missing on purpose
    assert advices == [
        Failure(
            code="python-parse-error",
            message="Failed to parse code due to invalid syntax: print(1",
            start_line=0,
            start_col=5,
            end_line=0,
            end_col=1,
        )
    ]


def test_python_sequential_linter_lint_lints_tree() -> None:
    linter = PythonSequentialLinter([DummyPythonLinter()], [], [])
    advices = list(linter.lint("print(1)"))
    assert advices == [Advice("dummy", "dummy advice", 0, 0, 0, 0), Advice("dummy", "dummy advice", 1, 1, 1, 1)]


def test_python_sequential_linter_lint_has_no_globals() -> None:
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    advices = list(linter.lint("print(1)"))
    assert advices == [Advice("globals", "", 0, 0, 0, 0)]


def test_python_sequential_linter_lint_has_one_global() -> None:
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    advices = list(linter.lint("a = 1"))
    assert advices == [Advice("globals", "a", 0, 0, 0, 0)]


def test_python_sequential_linter_lint_has_two_globals() -> None:
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    advices = list(linter.lint("a = 1;b = 2"))
    assert advices == [Advice("globals", "a,b", 0, 0, 0, 0)]


def test_python_sequential_linter_lint_is_stateless() -> None:
    """Globals from previous lint calls should not be part of later calls"""
    linter = PythonSequentialLinter([BodyNodesGlobalsLinter()], [], [])
    list(linter.lint("a = 1"))
    advices = list(linter.lint("b = 2"))
    assert advices == [Advice("globals", "b", 0, 0, 0, 0)]


class DummyDfsaPyCollector(DfsaPyCollector):
    """Dummy direct filesystem access collector yielding dummy advices for testing purpose."""

    def collect_dfsas_from_tree(self, tree: Tree) -> Iterable[DirectFsAccessNode]:
        dfsa = DirectFsAccess(path="test.py")
        node = NodeNG(0, 0, None, end_lineno=0, end_col_offset=0)
        yield DirectFsAccessNode(dfsa, node)


def test_dummy_dfsa_python_collector_collect_dfsas() -> None:
    linter = DummyDfsaPyCollector()
    dfsas = list(linter.collect_dfsas("print(1)"))
    assert dfsas == [DirectFsAccess(path="test.py")]


def test_dummy_dfsa_python_collector_collect_dfsas_warns_failure_due_to_parse_error(caplog) -> None:
    linter = DummyDfsaPyCollector()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.python.python_ast"):
        dfsas = list(linter.collect_dfsas("print(1"))  # Closing parenthesis is missing on purpose
    assert not dfsas
    assert "Failed to parse code due to invalid syntax: print(1" in caplog.messages


def test_python_sequential_linter_collect_dfsas() -> None:
    linter = PythonSequentialLinter([], [DummyDfsaPyCollector()], [])
    dfsas = list(linter.collect_dfsas("print(1)"))
    assert dfsas == [DirectFsAccess(path="test.py")]


class DummyTablePyCollector(TablePyCollector):
    """Dummy table collector yielding dummy used tables for testing purpose."""

    def collect_tables_from_tree(self, tree: Tree) -> Iterable[UsedTableNode]:
        dfsa = UsedTable(schema_name="test", table_name="test")
        node = NodeNG(0, 0, None, end_lineno=0, end_col_offset=0)
        yield UsedTableNode(dfsa, node)


def test_dummy_table_python_collector_collect_tables() -> None:
    linter = DummyTablePyCollector()
    used_tables = list(linter.collect_tables("print(1)"))
    assert used_tables == [UsedTable(schema_name="test", table_name="test")]


def test_dummy_table_python_collector_collect_tables_warns_failure_due_to_parse_error(caplog) -> None:
    linter = DummyTablePyCollector()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.python.python_ast"):
        used_tables = list(linter.collect_tables("print(1"))  # Closing parenthesis is missing on purpose
    assert not used_tables
    assert "Failed to parse code due to invalid syntax: print(1" in caplog.messages


def test_python_sequential_linter_collect_tables() -> None:
    linter = PythonSequentialLinter([], [], [DummyTablePyCollector()])
    used_tables = list(linter.collect_tables("print(1)"))
    assert used_tables == [UsedTable(schema_name="test", table_name="test")]


class DummyPythonLinter(PythonLinter):
    """Dummy python linter yielding dummy advices for testing purpose."""

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        yield Advice("dummy", "dummy advice", 0, 0, 0, 0)
        yield Advice("dummy", "dummy advice", 1, 1, 1, 1)


class BodyNodesGlobalsLinter(PythonLinter):
    """Get the node globals"""

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        globs = set()
        if isinstance(tree.node, Module):
            for node in tree.node.body:
                globs |= set(node.parent.globals.keys())
        yield Advice("globals", ",".join(sorted(globs)), 0, 0, 0, 0)
