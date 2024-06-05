from __future__ import annotations


import pytest
from astroid import Attribute, Call, Expr  # type: ignore
from databricks.labs.ucx.source_code.graph import DependencyProblem

from databricks.labs.ucx.source_code.linters.imports import ASTLinter, DbutilsLinter, TreeWalker


def test_linter_returns_empty_list_of_dbutils_notebook_run_calls():
    linter = ASTLinter.parse('')
    assert not DbutilsLinter.list_dbutils_notebook_run_calls(linter)


def test_linter_returns_list_of_dbutils_notebook_run_calls():
    code = """
dbutils.notebook.run("stuff")
for i in z:
    ww =   dbutils.notebook.run("toto")
"""
    linter = ASTLinter.parse(code)
    calls = DbutilsLinter.list_dbutils_notebook_run_calls(linter)
    assert {"toto", "stuff"} == {str(call.node.args[0].value) for call in calls}


def test_linter_returns_empty_list_of_imports():
    linter = ASTLinter.parse('')
    assert not DbutilsLinter.list_import_sources(linter, DependencyProblem)[0]


def test_linter_returns_import():
    linter = ASTLinter.parse('import x')
    assert ["x"] == [node.name for node in DbutilsLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns_import_from():
    linter = ASTLinter.parse('from x import z')
    assert ["x"] == [node.name for node in DbutilsLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns_import_module():
    linter = ASTLinter.parse('importlib.import_module("x")')
    assert ["x"] == [node.name for node in DbutilsLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns__import__():
    linter = ASTLinter.parse('importlib.__import__("x")')
    assert ["x"] == [node.name for node in DbutilsLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns_appended_absolute_paths():
    code = """
import sys
sys.path.append("absolute_path_1")
sys.path.append("absolute_path_2")
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_alias():
    code = """
import sys as stuff
stuff.path.append("absolute_path_1")
stuff.path.append("absolute_path_2")
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_path_alias():
    code = """
from sys import path as stuff
stuff.append("absolute_path")
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert "absolute_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths():
    code = """
import sys
import os
sys.path.append(os.path.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_alias():
    code = """
import sys
import os as stuff
sys.path.append(stuff.path.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_alias():
    code = """
import sys
from os import path as stuff
sys.path.append(stuff.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_import():
    code = """
import sys
from os.path import abspath
sys.path.append(abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_alias():
    code = """
import sys
from os.path import abspath as stuff
sys.path.append(stuff("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = DbutilsLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_extract_call_by_name(migration_index):
    linter = ASTLinter.parse("o.m1().m2().m3()")
    stmt = linter.first_statement()
    assert isinstance(stmt, Expr)
    act = ASTLinter.extract_call_by_name(stmt.value, "m2")
    assert isinstance(act, Call)
    assert isinstance(act.func, Attribute)
    assert act.func.attrname == "m2"


def test_extract_call_by_name_none(migration_index):
    linter = ASTLinter.parse("o.m1().m2().m3()")
    stmt = linter.first_statement()
    assert isinstance(stmt, Expr)
    act = ASTLinter.extract_call_by_name(stmt.value, "m5000")
    assert act is None


@pytest.mark.parametrize(
    "code, expected",
    [
        (
            """
name = "xyz"
dbutils.notebook.run(name)
""",
            "xyz",
        )
    ],
)
def test_infers_string_variable_value(code, expected):
    linter = ASTLinter.parse(code)
    calls = DbutilsLinter.list_dbutils_notebook_run_calls(linter)
    actual = list(call.get_notebook_path() for call in calls)
    assert [expected] == actual


def test_tree_walker_walks_nodes_once():
    nodes = set()
    count = 0
    linter = ASTLinter.parse("o.m1().m2().m3()")
    for node in TreeWalker.walk(linter.root):
        nodes.add(node)
        count += 1
    assert len(nodes) == count
