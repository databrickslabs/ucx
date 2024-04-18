import pytest

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code import languages
from databricks.labs.ucx.source_code.base import Deprecation, Advisory, Advice
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook_linter import NotebookLinter

index = MigrationIndex([])


@pytest.mark.parametrize(
    "lang, source, expected",
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
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=4,
                    start_col=0,
                    end_line=4,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/things/e/f/g',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=14,
                    start_col=23,
                    end_line=14,
                    end_col=40,
                ),
            ],
        ),
        (
            languages.Language.PYTHON,
            # 3 alerts
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
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=5,
                    start_col=23,
                    end_line=5,
                    end_col=40,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/foo',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=8,
                    start_col=0,
                    end_line=8,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/bar/e/f/g',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=20,
                    start_col=0,
                    end_line=20,
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
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=7,
                    start_col=7,
                    end_line=7,
                    end_col=16,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: /dbfs/mnt',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=8,
                    start_col=7,
                    end_line=8,
                    end_col=16,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: /mnt/',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=9,
                    start_col=7,
                    end_line=9,
                    end_col=12,
                ),
                Advisory(
                    code='dbfs-usage',
                    message='Possible deprecated file system path: dbfs:/...',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=10,
                    start_col=7,
                    end_line=10,
                    end_col=16,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /dbfs/mnt/data',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=11,
                    start_col=10,
                    end_line=11,
                    end_col=24,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /dbfs/mnt/data',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=13,
                    start_col=10,
                    end_line=13,
                    end_col=24,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/foo/bar',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=15,
                    start_col=19,
                    end_line=15,
                    end_col=31,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: dbfs:/mnt/foo/bar',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=16,
                    start_col=19,
                    end_line=16,
                    end_col=36,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: dbfs://mnt/foo/bar',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=17,
                    start_col=19,
                    end_line=17,
                    end_col=37,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/...',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=21,
                    start_col=0,
                    end_line=21,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /mnt/...',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=26,
                    start_col=0,
                    end_line=26,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /a/b/c',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=30,
                    start_col=0,
                    end_line=30,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /...',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
                    start_line=34,
                    start_col=0,
                    end_line=34,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: /dbfs/...',
                    source_type=Advice.MISSING_SOURCE_TYPE,
                    source_path=Advice.MISSING_SOURCE_PATH,
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
def test_notebook_linter(lang, source, expected):
    # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
    # Hence SQL statement advice offsets can be wrong because of comments and statements
    # over multiple lines.
    langs = Languages(index)
    linter = NotebookLinter.from_source(langs, source, lang)
    assert linter is not None
    gathered = list(linter.lint())
    assert gathered == expected


def test_notebook_linter_name():
    langs = Languages(index)
    source = """-- Databricks notebook source"""
    linter = NotebookLinter.from_source(langs, source, languages.Language.SQL)
    assert linter.name() == "notebook-linter"
