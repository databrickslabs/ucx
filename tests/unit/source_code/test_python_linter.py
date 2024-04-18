from databricks.labs.ucx.source_code.base import Location
from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter, ImportSource


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
    assert {"toto", "stuff"} == {str(call.args[0].value) for call in calls}


def test_linter_returns_empty_list_of_imports():
    linter = ASTLinter.parse('')
    assert not PythonLinter.list_import_sources(linter)


def test_linter_returns_import():
    linter = ASTLinter.parse('import x')
    assert [
        ImportSource(
            import_string="x",
            location=Location(code="import", message="", start_line=1, start_col=0, end_line=1, end_col=8),
        ),
    ] == PythonLinter.list_import_sources(linter)


def test_linter_returns_import_from():
    linter = ASTLinter.parse('from x import z')
    assert [
        ImportSource(
            import_string="x",
            location=Location(code="import from", message="", start_line=1, start_col=0, end_line=1, end_col=15),
        ),
    ] == PythonLinter.list_import_sources(linter)


def test_linter_returns_import_module():
    linter = ASTLinter.parse('importlib.import_module("x")')
    assert [
        ImportSource(
            import_string="x",
            location=Location(code="import_module", message="", start_line=1, start_col=0, end_line=1, end_col=28),
        ),
    ] == PythonLinter.list_import_sources(linter)


def test_linter_returns__import__():
    linter = ASTLinter.parse('importlib.__import__("x")')
    assert [
        ImportSource(
            import_string="x",
            location=Location(code="__import__", message="", start_line=1, start_col=0, end_line=1, end_col=25),
        ),
    ] == PythonLinter.list_import_sources(linter)


def test_linter_returns_appended_absolute_paths():
    code = """
import sys
sys.path.append("absolute_path_1")
sys.path.append("absolute_path_2")
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_alias():
    code = """
import sys as stuff
stuff.path.append("absolute_path_1")
stuff.path.append("absolute_path_2")
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert ["absolute_path_1", "absolute_path_2"] == [p.path for p in appended]


def test_linter_returns_appended_absolute_paths_with_sys_path_alias():
    code = """
from sys import path as stuff
stuff.append("absolute_path")
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert "absolute_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths():
    code = """
import sys
import os
sys.path.append(os.path.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_alias():
    code = """
import sys
import os as stuff
sys.path.append(stuff.path.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_alias():
    code = """
import sys
from os import path as stuff
sys.path.append(stuff.abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_import():
    code = """
import sys
from os.path import abspath
sys.path.append(abspath("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert "relative_path" in [p.path for p in appended]


def test_linter_returns_appended_relative_paths_with_os_path_abspath_alias():
    code = """
import sys
from os.path import abspath as stuff
sys.path.append(stuff("relative_path"))
"""
    linter = ASTLinter.parse(code)
    appended = PythonLinter.list_appended_sys_paths(linter)
    assert "relative_path" in [p.path for p in appended]
