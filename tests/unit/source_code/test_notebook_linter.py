import pytest

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code import languages
from databricks.labs.ucx.source_code.base import Deprecation, Advisory
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.notebook_linter import NotebookLinter

index = MigrationIndex([])


@pytest.mark.parametrize(
    "lang, source, expected, adjusted",
    [
        # 2 alerts
        (
            languages.Language.SQL,
            """-- Databricks notebook source
-- MAGIC %md # This is a SQL notebook, that has Python cell embedded

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/mnt/whatever`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.csv('/mnt/things/e/f/g'))
""",
            [
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/whatever',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/things/e/f/g',
                    start_line=2,
                    start_col=23,
                    end_line=2,
                    end_col=40,
                ),
            ],
            [
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/whatever',
                    start_line=4,
                    start_col=0,
                    end_line=4,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/things/e/f/g',
                    start_line=10,
                    start_col=23,
                    end_line=10,
                    end_col=40,
                ),
            ],
        ),
        (
            languages.Language.PYTHON,
            """# Databricks notebook source
# MAGIC %md # This is a Python notebook, that has SQL cell embedded

# COMMAND ----------

display(spark.read.csv('/mnt/things/e/f/g'))

# COMMAND ----------

# MAGIC %sql  SELECT * FROM csv.`dbfs:/mnt/foo`

# COMMAND ----------

# MAGIC %md mess around with formatting

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC   csv.`dbfs:/mnt/bar/e/f/g` 
# MAGIC WHERE _c1 > 5
""",
            [
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/things/e/f/g',
                    start_line=1,
                    start_col=23,
                    end_line=1,
                    end_col=40,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/bar/e/f/g',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
            ],
            [
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/things/e/f/g',
                    start_line=5,
                    start_col=23,
                    end_line=5,
                    end_col=40,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/bar/e/f/g',
                    start_line=16,
                    start_col=0,
                    end_line=16,
                    end_col=1024,
                ),
            ],
        ),
        (
            languages.Language.SQL,
            """-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Test notebook for DBFS discovery in Notebooks

-- COMMAND ----------
-- DBTITLE 1,A Python cell that references DBFS
-- MAGIC %python
-- MAGIC DBFS = "dbfs:/..."
-- MAGIC DBFS = "/dbfs/mnt"
-- MAGIC DBFS = "/mnt/"
-- MAGIC DBFS = "dbfs:/..."
-- MAGIC load_data('/dbfs/mnt/data')
-- MAGIC load_data('/data')
-- MAGIC load_data('/dbfs/mnt/data', '/data')
-- MAGIC # load_data('/dbfs/mnt/data', '/data')
-- MAGIC spark.read.parquet("/mnt/foo/bar")
-- MAGIC spark.read.parquet("dbfs:/mnt/foo/bar")
-- MAGIC spark.read.parquet("dbfs://mnt/foo/bar")
-- MAGIC # Would need a stateful linter to detect this next one
-- MAGIC spark.read.parquet(DBFS)

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM parquet.`dbfs:/...` LIMIT 10

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
    """,
            [
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: dbfs:/...',
                    start_line=3,
                    start_col=7,
                    end_line=3,
                    end_col=16,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: /dbfs/mnt',
                    start_line=4,
                    start_col=7,
                    end_line=4,
                    end_col=16,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: /mnt/',
                    start_line=5,
                    start_col=7,
                    end_line=5,
                    end_col=12,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: dbfs:/...',
                    start_line=6,
                    start_col=7,
                    end_line=6,
                    end_col=16,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /dbfs/mnt/data',
                    start_line=7,
                    start_col=10,
                    end_line=7,
                    end_col=24,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /dbfs/mnt/data',
                    start_line=9,
                    start_col=10,
                    end_line=9,
                    end_col=24,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/foo/bar',
                    start_line=11,
                    start_col=19,
                    end_line=11,
                    end_col=31,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: dbfs:/mnt/foo/bar',
                    start_line=12,
                    start_col=19,
                    end_line=12,
                    end_col=36,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: dbfs://mnt/foo/bar',
                    start_line=13,
                    start_col=19,
                    end_line=13,
                    end_col=37,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/...',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /mnt/...',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /a/b/c',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /...',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /dbfs/...',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                ),
            ],
            [
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: dbfs:/...',
                    start_line=7,
                    start_col=7,
                    end_line=7,
                    end_col=16,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: /dbfs/mnt',
                    start_line=8,
                    start_col=7,
                    end_line=8,
                    end_col=16,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: /mnt/',
                    start_line=9,
                    start_col=7,
                    end_line=9,
                    end_col=12,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: dbfs:/...',
                    start_line=10,
                    start_col=7,
                    end_line=10,
                    end_col=16,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /dbfs/mnt/data',
                    start_line=11,
                    start_col=10,
                    end_line=11,
                    end_col=24,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /dbfs/mnt/data',
                    start_line=13,
                    start_col=10,
                    end_line=13,
                    end_col=24,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/foo/bar',
                    start_line=15,
                    start_col=19,
                    end_line=15,
                    end_col=31,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: dbfs:/mnt/foo/bar',
                    start_line=16,
                    start_col=19,
                    end_line=16,
                    end_col=36,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: dbfs://mnt/foo/bar',
                    start_line=17,
                    start_col=19,
                    end_line=17,
                    end_col=37,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/...',
                    start_line=21,
                    start_col=0,
                    end_line=21,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /mnt/...',
                    start_line=26,
                    start_col=0,
                    end_line=26,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /a/b/c',
                    start_line=30,
                    start_col=0,
                    end_line=30,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /...',
                    start_line=34,
                    start_col=0,
                    end_line=34,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /dbfs/...',
                    start_line=38,
                    start_col=0,
                    end_line=38,
                    end_col=1024,
                ),
            ],
        ),
        # Add more test cases here
    ],
)
def test_notebook_linter(lang, source, expected, adjusted):
    langs = Languages(index)
    linter = NotebookLinter.from_source(langs, source, lang)
    assert linter is not None
    raw_list = list(linter.lint())
    assert raw_list == expected
    adjusted_list = list(linter.adjust_advices(raw_list))
    assert adjusted_list == adjusted


def test_notebook_linter_name():
    langs = Languages(index)
    source = """-- Databricks notebook source"""
    linter = NotebookLinter.from_source(langs, source, languages.Language.SQL)
    assert linter.name() == "notebook-linter"


def test_notebook_linter_from_notebook():
    langs = Languages(index)
    source = """-- Databricks notebook source"""
    notebook = Notebook.parse("", source, languages.Language.SQL)
    assert notebook is not None
    linter = NotebookLinter.from_notebook(langs, notebook)
    assert linter is not None
