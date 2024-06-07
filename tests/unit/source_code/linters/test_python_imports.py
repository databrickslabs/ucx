from __future__ import annotations


import pytest
from astroid import Attribute, Call, Const, Expr  # type: ignore
from databricks.labs.ucx.source_code.graph import DependencyProblem

from databricks.labs.ucx.source_code.linters.imports import DbutilsLinter, ImportSource, SysPathChange
from databricks.labs.ucx.source_code.linters.python_ast import Tree


def test_linter_returns_empty_list_of_dbutils_notebook_run_calls():
    tree = Tree.parse('')
    assert not DbutilsLinter.list_dbutils_notebook_run_calls(tree)


def test_linter_returns_list_of_dbutils_notebook_run_calls():
    code = """
dbutils.notebook.run("stuff")
for i in z:
    ww =   dbutils.notebook.run("toto")
"""
    tree = Tree.parse(code)
    calls = DbutilsLinter.list_dbutils_notebook_run_calls(tree)
    assert {"toto", "stuff"} == {str(call.node.args[0].value) for call in calls}


def test_linter_returns_empty_list_of_imports():
    tree = Tree.parse('')
    assert not ImportSource.extract_from_tree(tree, DependencyProblem)[0]


def test_linter_returns_import():
    tree = Tree.parse('import x')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem)[0]]


def test_linter_returns_import_from():
    tree = Tree.parse('from x import z')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem)[0]]


def test_linter_returns_import_module():
    tree = Tree.parse('importlib.import_module("x")')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem)[0]]


def test_linter_returns__import__():
    tree = Tree.parse('importlib.__import__("x")')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem)[0]]


def test_linter_returns_appended_absolute_paths():
    code = """
import sys
sys.path.append("absolute_path_1")
sys.path.append("absolute_path_2")
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_alias():
    code = """
import sys as stuff
stuff.path.append("absolute_path_1")
stuff.path.append("absolute_path_2")
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_path_alias():
    code = """
from sys import path as stuff
stuff.append("absolute_path")
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert "absolute_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths():
    code = """
import sys
import os
sys.path.append(os.path.abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_alias():
    code = """
import sys
import os as stuff
sys.path.append(stuff.path.abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_alias():
    code = """
import sys
from os import path as stuff
sys.path.append(stuff.abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_import():
    code = """
import sys
from os.path import abspath
sys.path.append(abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_alias():
    code = """
import sys
from os.path import abspath as stuff
sys.path.append(stuff("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(tree)
    assert "relative_path" in [p.path for p in appended]


def test_extract_call_by_name():
    tree = Tree.parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.extract_call_by_name(stmt.value, "m2")
    assert isinstance(act, Call)
    assert isinstance(act.func, Attribute)
    assert act.func.attrname == "m2"


def test_extract_call_by_name_none():
    tree = Tree.parse("o.m1().m2().m3()")
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.extract_call_by_name(stmt.value, "m5000")
    assert act is None


@pytest.mark.parametrize(
    "code, arg_index, arg_name, expected",
    [
        ("o.m1()", 1, "second", None),
        ("o.m1(3)", 1, "second", None),
        ("o.m1(first=3)", 1, "second", None),
        ("o.m1(4, 3)", None, None, None),
        ("o.m1(4, 3)", None, "second", None),
        ("o.m1(4, 3)", 1, "second", 3),
        ("o.m1(4, 3)", 1, None, 3),
        ("o.m1(first=4, second=3)", 1, "second", 3),
        ("o.m1(second=3, first=4)", 1, "second", 3),
        ("o.m1(second=3, first=4)", None, "second", 3),
        ("o.m1(second=3)", 1, "second", 3),
        ("o.m1(4, 3, 2)", 1, "second", 3),
    ],
)
def test_linter_gets_arg(code, arg_index, arg_name, expected):
    tree = Tree.parse(code)
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.get_arg(stmt.value, arg_index, arg_name)
    if expected is None:
        assert act is None
    else:
        assert isinstance(act, Const)
        assert act.value == expected


@pytest.mark.parametrize(
    "code, expected",
    [
        ("o.m1()", 0),
        ("o.m1(3)", 1),
        ("o.m1(first=3)", 1),
        ("o.m1(3, 3)", 2),
        ("o.m1(first=3, second=3)", 2),
        ("o.m1(3, second=3)", 2),
        ("o.m1(3, *b, **c, second=3)", 4),
    ],
)
def test_args_count(code, expected):
    tree = Tree.parse(code)
    stmt = tree.first_statement()
    assert isinstance(stmt, Expr)
    assert isinstance(stmt.value, Call)
    act = Tree.args_count(stmt.value)
    assert act == expected


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
    tree = Tree.parse(code)
    calls = DbutilsLinter.list_dbutils_notebook_run_calls(tree)
    actual = list(call.get_notebook_path() for call in calls)
    assert [expected] == actual


def test_tree_walker_walks_nodes_once():
    nodes = set()
    count = 0
    tree = Tree.parse("o.m1().m2().m3()")
    for node in tree.walk():
        nodes.add(node)
        count += 1
    assert len(nodes) == count
