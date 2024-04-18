from databricks.labs.ucx.source_code.base import Advisory, Advice
from databricks.labs.ucx.source_code.dependencies import SourceContainer
from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter
from tests.unit import _load_sources


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
    assert [] == PythonLinter.list_import_sources(linter)


def test_linter_returns_import():
    linter = ASTLinter.parse('import x')
    names = [source[0] for source in PythonLinter.list_import_sources(linter)]
    assert ["x"] == names


def test_linter_returns_import_from():
    linter = ASTLinter.parse('from x import z')
    names = [source[0] for source in PythonLinter.list_import_sources(linter)]
    assert ["x"] == names


def test_linter_returns_import_module():
    linter = ASTLinter.parse('importlib.import_module("x")')
    names = [source[0] for source in PythonLinter.list_import_sources(linter)]
    assert ["x"] == names


def test_linter_returns__import__():
    linter = ASTLinter.parse('importlib.__import__("x")')
    names = [source[0] for source in PythonLinter.list_import_sources(linter)]
    assert ["x"] == names


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


def test_linter_detects_manual_migration_in_dbutils_notebook_run_in_python_code_():
    sources: list[str] = _load_sources(SourceContainer, "run_notebooks.py.txt")
    linter = PythonLinter()
    advices = list(linter.lint(sources[0]))
    assert [
        Advisory(
            code='dbutils-notebook-run-dynamic',
            message="Path for 'dbutils.notebook.run' is not a constant and requires adjusting the notebook path",
            source_type=Advice.MISSING_SOURCE_TYPE,
            source_path=Advice.MISSING_SOURCE_PATH,
            start_line=14,
            start_col=13,
            end_line=14,
            end_col=50,
        )
    ] == advices


def test_linter_detects_automatic_migration_in_dbutils_notebook_run_in_python_code_():
    sources: list[str] = _load_sources(SourceContainer, "root4.py.txt")
    linter = PythonLinter()
    advices = list(linter.lint(sources[0]))
    assert [
        Advisory(
            code='dbutils-notebook-run-literal',
            message="Call to 'dbutils.notebook.run' will be migrated automatically",
            source_type=Advice.MISSING_SOURCE_TYPE,
            source_path=Advice.MISSING_SOURCE_PATH,
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=38,
        )
    ] == advices


def test_linter_detects_multiple_calls_to_dbutils_notebook_run_in_python_code_():
    source = """
import stuff
do_something_with_stuff(stuff)
stuff2 = dbutils.notebook.run("where is notebook 1?")
stuff3 = dbutils.notebook.run("where is notebook 2?")
"""
    linter = PythonLinter()
    advices = list(linter.lint(source))
    assert len(advices) == 2


def test_linter_does_not_detect_partial_call_to_dbutils_notebook_run_in_python_code_():
    source = """
import stuff
do_something_with_stuff(stuff)
stuff2 = notebook.run("where is notebook 1?")
"""
    linter = PythonLinter()
    advices = list(linter.lint(source))
    assert len(advices) == 0
