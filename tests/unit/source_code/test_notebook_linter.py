# Place holder for notebook linter testing
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code import languages
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import WorkspaceNotebook
from databricks.labs.ucx.source_code.notebook_linter import NotebookLinter


def test_notebook_linter():
    notebook_code = """-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Test notebook for DBFS discovery in Notebooks

-- COMMAND ----------

-- DBTITLE 1,A Python cell that references DBFS
-- MAGIC %python
-- MAGIC DBFS = "dbfs:/..."
-- MAGIC DBFS = "/dbfs/mnt"
-- MAGIC DBFS = "/mnt/")
-- MAGIC DBFS = "dbfs:/..."
-- MAGIC load_data('/dbfs/mnt/data')", 1),
-- MAGIC load_data('/data')", 0),
-- MAGIC load_data('/dbfs/mnt/data', '/data')", 1),
-- MAGIC # load_data('/dbfs/mnt/data', '/data')", 0),
-- MAGIC spark.read.parquet("/mnt/foo/bar")', 1),
-- MAGIC spark.read.parquet("dbfs:/mnt/foo/bar")', 1),
-- MAGIC spark.read.parquet("dbfs://mnt/foo/bar")', 1),
-- MAGIC # Would need a stateful linter to detect this next one
-- MAGIC spark.read.parquet(DBFS)'

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM parquet.`dbfs:/...` LIMIT 10'

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS
SELECT * FROM delta.`/mnt/...` WHERE foo > 6

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS
        SELECT * FROM json.`/a/b/c` WHERE foo > 6

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS
        DELETE FROM json.`/...` WHERE foo = 'bar'

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS

MERGE INTO delta.`/dbfs/...` t USING source ON t.key = source.key WHEN MATCHED THEN DELETE
    """

    notebook = WorkspaceNotebook.parse("", notebook_code, languages.Language.SQL)
    assert notebook is not None
    # Note that this is a place holder test, the actual implementation is not yet done
    # will obviously lint each cell in the notebook and raise an advisory if any reference
    # to DBFS is found
    for cell in notebook.cells:
        print(cell)
        print(cell.original_code)
        print(cell.language)


def test_notebook_linter_name():
    langs = create_autospec(Languages)
    linter = NotebookLinter(langs)
    assert linter.name() == "notebook-linter"
