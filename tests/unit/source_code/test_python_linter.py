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
    assert {"toto", "stuff"} == {str(call.args[0].value) for call in calls}


def test_linter_returns_empty_list_of_imports():
    linter = ASTLinter.parse('')
    assert [] == PythonLinter.list_import_sources(linter)


def test_linter_returns_import():
    linter = ASTLinter.parse('import x')
    assert ["x"] == PythonLinter.list_import_sources(linter)


def test_linter_returns_import_from():
    linter = ASTLinter.parse('from x import z')
    assert ["x"] == PythonLinter.list_import_sources(linter)


def test_linter_returns_import_module():
    linter = ASTLinter.parse('importlib.import_module("x")')
    assert ["x"] == PythonLinter.list_import_sources(linter)


def test_linter_returns__import__():
    linter = ASTLinter.parse('importlib.__import__("x")')
    assert ["x"] == PythonLinter.list_import_sources(linter)
