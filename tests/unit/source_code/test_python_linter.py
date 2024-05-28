from __future__ import annotations

import ast
import pytest
from databricks.labs.ucx.source_code.graph import DependencyProblem

from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter


def test_linter_returns_empty_list_of_dbutils_notebook_run_calls():
    linter = ASTLinter.parse('')
    assert not PythonLinter.list_dbutils_notebook_run_calls(linter)


def test_linter_returns_list_of_dbutils_notebook_run_calls():
    code = """
dbutils.notebook.run("stuff")
for i in z:
    ww =   dbutils.notebook.run("toto")
"""
    linter = ASTLinter.parse(code)
    calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
    assert {"toto", "stuff"} == {str(call.node.args[0].value) for call in calls}


def test_linter_returns_empty_list_of_imports():
    linter = ASTLinter.parse('')
    assert [] == PythonLinter.list_import_sources(linter, DependencyProblem)[0]


def test_linter_returns_import():
    linter = ASTLinter.parse('import x')
    assert ["x"] == [node.name for node in PythonLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns_import_from():
    linter = ASTLinter.parse('from x import z')
    assert ["x"] == [node.name for node in PythonLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns_import_module():
    linter = ASTLinter.parse('importlib.import_module("x")')
    assert ["x"] == [node.name for node in PythonLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns__import__():
    linter = ASTLinter.parse('importlib.__import__("x")')
    assert ["x"] == [node.name for node in PythonLinter.list_import_sources(linter, DependencyProblem)[0]]


def test_linter_returns_appended_absolute_paths():
    code = """
import sys
sys.path.append("absolute_path_1")
sys.path.append("absolute_path_2")
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_alias():
    code = """
import sys as stuff
stuff.path.append("absolute_path_1")
stuff.path.append("absolute_path_2")
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_path_alias():
    code = """
from sys import path as stuff
stuff.append("absolute_path")
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert "absolute_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths():
    code = """
import sys
import os
sys.path.append(os.path.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_alias():
    code = """
import sys
import os as stuff
sys.path.append(stuff.path.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_alias():
    code = """
import sys
from os import path as stuff
sys.path.append(stuff.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_import():
    code = """
import sys
from os.path import abspath
sys.path.append(abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_alias():
    code = """
import sys
from os.path import abspath as stuff
sys.path.append(stuff("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_sys_path_changes(linter)
    assert "relative_path" in [p.path for p in appended]


def get_statement_node(stmt: str) -> ast.stmt:
    node = ast.parse(stmt)
    return node.body[0]


@pytest.mark.parametrize("stmt", ["o.m1().m2().m3()", "a = o.m1().m2().m3()"])
def test_extract_callchain(migration_index, stmt):
    node = get_statement_node(stmt)
    act = ASTLinter(node).extract_callchain()
    assert isinstance(act, ast.Call)
    assert isinstance(act.func, ast.Attribute)
    assert act.func.attr == "m3"


@pytest.mark.parametrize("stmt", ["a = 3", "[x+1 for x in xs]"])
def test_extract_callchain_none(migration_index, stmt):
    node = get_statement_node(stmt)
    act = ASTLinter(node).extract_callchain()
    assert act is None


def test_extract_call_by_name(migration_index):
    callchain = get_statement_node("o.m1().m2().m3()").value
    act = ASTLinter(callchain).extract_call_by_name("m2")
    assert isinstance(act, ast.Call)
    assert isinstance(act.func, ast.Attribute)
    assert act.func.attr == "m2"


def test_extract_call_by_name_none(migration_index):
    callchain = get_statement_node("o.m1().m2().m3()").value
    act = ASTLinter(callchain).extract_call_by_name("m5000")
    assert act is None


@pytest.mark.parametrize(
    "param",
    [
        {"stmt": "o.m1()", "arg_index": 1, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(3)", "arg_index": 1, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(first=3)", "arg_index": 1, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(4, 3)", "arg_index": None, "arg_name": None, "expected": None},
        {"stmt": "o.m1(4, 3)", "arg_index": None, "arg_name": "second", "expected": None},
        {"stmt": "o.m1(4, 3)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(4, 3)", "arg_index": 1, "arg_name": None, "expected": 3},
        {"stmt": "o.m1(first=4, second=3)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(second=3, first=4)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(second=3, first=4)", "arg_index": None, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(second=3)", "arg_index": 1, "arg_name": "second", "expected": 3},
        {"stmt": "o.m1(4, 3, 2)", "arg_index": 1, "arg_name": "second", "expected": 3},
    ],
)
def test_get_arg(migration_index, param):
    call = get_statement_node(param["stmt"]).value
    act = ASTLinter(call).get_arg(param["arg_index"], param["arg_name"])
    if param["expected"] is None:
        assert act is None
    else:
        assert isinstance(act, ast.Constant)
        assert act.value == param["expected"]


@pytest.mark.parametrize(
    "param",
    [
        {"stmt": "o.m1()", "expected": 0},
        {"stmt": "o.m1(3)", "expected": 1},
        {"stmt": "o.m1(first=3)", "expected": 1},
        {"stmt": "o.m1(3, 3)", "expected": 2},
        {"stmt": "o.m1(first=3, second=3)", "expected": 2},
        {"stmt": "o.m1(3, second=3)", "expected": 2},
        {"stmt": "o.m1(3, *b, **c, second=3)", "expected": 4},
    ],
)
def test_args_count(migration_index, param):
    call = get_statement_node(param["stmt"]).value
    act = ASTLinter(call).args_count()
    assert param["expected"] == act


@pytest.mark.parametrize(
    "param",
    [
        {"stmt": "a = x", "expected": False},
        {"stmt": "a = 3", "expected": False},
        {"stmt": "a = 'None'", "expected": False},
        {"stmt": "a = None", "expected": True},
    ],
)
def test_is_none(migration_index, param):
    val = get_statement_node(param["stmt"]).value
    act = ASTLinter(val).is_none()
    assert param["expected"] == act


@pytest.mark.parametrize(
    "code, expected",
    [
        ( """
name = "xyz"
dbutils.notebook.run(name)
""", "xyz")
    ],
)
def test_infers_string_variable_value(code, expected):
    linter = ASTLinter.parse(code)
    calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
    actual = list([ call.get_constant_path() for call in calls ])
    assert [ expected ] == actual
