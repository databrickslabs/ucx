from __future__ import annotations

from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.linters.files import FileLoader

from databricks.labs.ucx.source_code.linters.imports import DbutilsPyLinter, ImportSource, SysPathChange
from databricks.labs.ucx.source_code.python.python_analyzer import PythonCodeAnalyzer
from databricks.labs.ucx.source_code.python.python_ast import Tree


def test_linter_returns_empty_list_of_dbutils_notebook_run_calls() -> None:
    tree = Tree.parse('')
    assert not DbutilsPyLinter.list_dbutils_notebook_run_calls(tree)


def test_linter_returns_list_of_dbutils_notebook_run_calls() -> None:
    code = """
dbutils.notebook.run("stuff")
for i in z:
    ww =   dbutils.notebook.run("toto")
"""
    tree = Tree.parse(code)
    calls = DbutilsPyLinter.list_dbutils_notebook_run_calls(tree)
    assert {"toto", "stuff"} == {str(call.node.args[0].value) for call in calls}


def test_linter_returns_empty_list_of_imports() -> None:
    tree = Tree.parse('')
    assert not ImportSource.extract_from_tree(tree, DependencyProblem.from_node)[0]


def test_linter_returns_import() -> None:
    tree = Tree.parse('import x')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem.from_node)[0]]


def test_linter_returns_import_from() -> None:
    tree = Tree.parse('from x import z')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem.from_node)[0]]


def test_linter_returns_import_module() -> None:
    tree = Tree.parse('importlib.import_module("x")')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem.from_node)[0]]


def test_linter_returns__import__() -> None:
    tree = Tree.parse('importlib.__import__("x")')
    assert ["x"] == [node.name for node in ImportSource.extract_from_tree(tree, DependencyProblem.from_node)[0]]


def test_linter_returns_appended_absolute_paths() -> None:
    code = """
import sys
sys.path.append("absolute_path_1")
sys.path.append("absolute_path_2")
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_alias() -> None:
    code = """
import sys as stuff
stuff.path.append("absolute_path_1")
stuff.path.append("absolute_path_2")
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_path_alias() -> None:
    code = """
from sys import path as stuff
stuff.append("absolute_path")
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert "absolute_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths() -> None:
    code = """
import sys
import os
sys.path.append(os.path.abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_alias() -> None:
    code = """
import sys
import os as stuff
sys.path.append(stuff.path.abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_alias() -> None:
    code = """
import sys
from os import path as stuff
sys.path.append(stuff.abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_import() -> None:
    code = """
import sys
from os.path import abspath
sys.path.append(abspath("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_alias() -> None:
    code = """
import sys
from os.path import abspath as stuff
sys.path.append(stuff("relative_path"))
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_inferred_paths() -> None:
    code = """
import sys
path = "absolute_path_1"
sys.path.append(path)
"""
    tree = Tree.parse(code)
    appended = SysPathChange.extract_from_tree(CurrentSessionState(), tree)
    assert ["absolute_path_1"] == [p.path for p in appended]


@pytest.mark.parametrize(
    "code, expected",
    [
        (
            """
name = "xyz"
dbutils.notebook.run(name)
""",
            ["xyz"],
        ),
        (
            """
name = "xyz" + "-" + "abc"
dbutils.notebook.run(name)
""",
            ["xyz-abc"],
        ),
        (
            """
names = ["abc", "xyz"]
for name in names:
    dbutils.notebook.run(name)
""",
            ["abc", "xyz"],
        ),
        (
            """
def foo(): return "bar"
name = foo()
dbutils.notebook.run(name)
""",
            ["bar"],
        ),
    ],
)
def test_infers_dbutils_notebook_run_dynamic_value(code, expected) -> None:
    tree = Tree.parse(code)
    calls = DbutilsPyLinter.list_dbutils_notebook_run_calls(tree)
    all_paths: list[str] = []
    for call in calls:
        _, paths = call.get_notebook_paths(CurrentSessionState())
        all_paths.extend(paths)
    assert all_paths == expected


def test_failing_import_in_try_except_is_silent(simple_dependency_resolver, mock_path_lookup) -> None:
    code = """
try:
    import missing_library
except ImportError as e:
    pass
"""
    dep = Dependency(FileLoader(), Path(""))
    graph = DependencyGraph(dep, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    analyzer = PythonCodeAnalyzer(graph.new_dependency_graph_context(), code)
    problems = analyzer.build_graph()
    assert not problems
