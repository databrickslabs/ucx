from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph
from databricks.labs.ucx.source_code.linters.files import FileLoader
from databricks.labs.ucx.source_code.notebooks.sources import Notebook


def test_malformed_pip_cell_is_supported(simple_ctx):
    source = """# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to illustrate the order of execution. Happy exploring!

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

"""
    notebook = Notebook.parse(Path(""), source=source, default_language=Language.PYTHON)
    dependency = Dependency(FileLoader(), Path(""))
    parent = DependencyGraph(
        dependency, None, simple_ctx.dependency_resolver, simple_ctx.path_lookup, CurrentSessionState()
    )
    problems = notebook.build_dependency_graph(parent)
    assert not problems
