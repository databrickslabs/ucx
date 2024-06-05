import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Deprecation, Advisory, Advice
from databricks.labs.ucx.source_code.notebooks.sources import NotebookLinter

index = MigrationIndex([])


@pytest.mark.parametrize(
    "lang, source, expected",
    [
        # 2 alerts
        (
            Language.SQL,
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
                    start_line=4,
                    start_col=0,
                    end_line=4,
                    end_col=1024,
                ),
                Deprecation(
                    code='direct-filesystem-access',
                    message='The use of default dbfs: references is deprecated: ' '/mnt/things/e/f/g',
                    start_line=14,
                    start_col=8,
                    end_line=14,
                    end_col=43,
                ),
                Deprecation(
                    code='dbfs-usage',
                    message='Deprecated file system path in call to: /mnt/things/e/f/g',
                    start_line=14,
                    start_col=23,
                    end_line=14,
                    end_col=40,
                ),
            ],
        ),
        (
            Language.PYTHON,
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
                    code='direct-filesystem-access',
                    message='The use of default dbfs: references is deprecated: ' '/mnt/things/e/f/g',
                    start_line=5,
                    start_col=8,
                    end_line=5,
                    end_col=43,
                ),
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
                    message='The use of DBFS is deprecated: dbfs:/mnt/foo',
                    start_line=8,
                    start_col=0,
                    end_line=8,
                    end_col=1024,
                ),
                Deprecation(
                    code='dbfs-query',
                    message='The use of DBFS is deprecated: dbfs:/mnt/bar/e/f/g',
                    start_line=20,
                    start_col=0,
                    end_line=20,
                    end_col=1024,
                ),
            ],
        ),
        (
            Language.SQL,
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
                Deprecation(
                    code='direct-filesystem-access',
                    message='The use of default dbfs: references is deprecated: /mnt/foo/bar',
                    start_line=15,
                    start_col=0,
                    end_line=15,
                    end_col=34,
                ),
                Deprecation(
                    code='direct-filesystem-access',
                    message='The use of direct filesystem references is deprecated: dbfs:/mnt/foo/bar',
                    start_line=16,
                    start_col=0,
                    end_line=16,
                    end_col=39,
                ),
                Deprecation(
                    code='direct-filesystem-access',
                    message='The use of direct filesystem references is deprecated: dbfs://mnt/foo/bar',
                    start_line=17,
                    start_col=0,
                    end_line=17,
                    end_col=40,
                ),
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
def test_notebook_linter(lang, source, expected):
    # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
    # Hence SQL statement advice offsets can be wrong because of comments and statements
    # over multiple lines.
    linter = NotebookLinter.from_source(index, source, lang)
    assert linter is not None
    gathered = list(linter.lint())
    assert gathered == expected


def test_notebook_linter_name():
    source = """-- Databricks notebook source"""
    linter = NotebookLinter.from_source(index, source, Language.SQL)
    assert linter.name() == "notebook-linter"


@pytest.mark.parametrize(
    "lang, source, expected",
    [
        (
            Language.SQL,
            """-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Test notebook for Use tracking in Notebooks

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that changes the DB

USE different_db

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references tables

SELECT * FROM  testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that changes the DB to one we migrate from

USE old

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references tables

SELECT * FROM  testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references tables

SELECT * FROM  stuff LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A Python cell that uses calls to change the USE
-- MAGIC %python
-- MAGIC # This is a Python cell that uses calls to change the USE...

spark.sql("use different_db")

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM old.testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that changes the DB to the default

USE default

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references DBFS

SELECT * FROM testtable LIMIT 10

-- COMMAND ----------
-- DBTITLE 1,A SQL cell that references tables

MERGE INTO catalog.schema.testtable t USING source ON t.key = source.key WHEN MATCHED THEN DELETE
    """,
            [
                Deprecation(
                    code='table-migrate',
                    message='Table different_db.testtable is migrated to cata2.newspace.table in Unity Catalog',
                    start_line=9,
                    start_col=0,
                    end_line=9,
                    end_col=1024,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table old.testtable is migrated to cata3.newspace.table in Unity Catalog',
                    start_line=19,
                    start_col=0,
                    end_line=19,
                    end_col=1024,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table old.stuff is migrated to brand.new.things in Unity Catalog',
                    start_line=24,
                    start_col=0,
                    end_line=24,
                    end_col=1024,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table different_db.testtable is migrated to ' 'cata2.newspace.table in Unity Catalog',
                    start_line=36,
                    start_col=0,
                    end_line=36,
                    end_col=1024,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table old.testtable is migrated to cata3.newspace.table in Unity Catalog',
                    start_line=41,
                    start_col=0,
                    end_line=41,
                    end_col=1024,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table default.testtable is migrated to cata.nondefault.table in Unity Catalog',
                    start_line=51,
                    start_col=0,
                    end_line=51,
                    end_col=1024,
                ),
            ],
        ),
        (
            Language.PYTHON,
            """# Databricks notebook source
--- MAGIC %md
-- MAGIC #Test notebook for Use tracking in Notebooks

# COMMAND ----------

display(spark.table('people')) # we are looking at default.people table

# COMMAND ----------

# MAGIC %sql USE something

# COMMAND ----------

display(spark.table('persons')) # we are looking at something.persons table

# COMMAND ----------

spark.sql('USE whatever')

# COMMAND ----------

display(spark.table('kittens')) # we are looking at whatever.kittens table

# COMMAND ----------

spark.range(10).saveAsTable('numbers') # we are saving to whatever.numbers table.""",
            [
                Deprecation(
                    code='table-migrate',
                    message='Table people is migrated to cata4.nondefault.newpeople in Unity Catalog',
                    start_line=6,
                    start_col=8,
                    end_line=6,
                    end_col=29,
                ),
                Advice(
                    code='table-migrate',
                    message='The default format changed in Databricks Runtime 8.0, from Parquet to Delta',
                    start_line=6,
                    start_col=8,
                    end_line=6,
                    end_col=29,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table persons is migrated to cata4.newsomething.persons in Unity Catalog',
                    start_line=14,
                    start_col=8,
                    end_line=14,
                    end_col=30,
                ),
                Advice(
                    code='table-migrate',
                    message='The default format changed in Databricks Runtime 8.0, from Parquet to Delta',
                    start_line=14,
                    start_col=8,
                    end_line=14,
                    end_col=30,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table kittens is migrated to cata4.felines.toms in Unity Catalog',
                    start_line=22,
                    start_col=8,
                    end_line=22,
                    end_col=30,
                ),
                Advice(
                    code='table-migrate',
                    message='The default format changed in Databricks Runtime 8.0, from Parquet to Delta',
                    start_line=22,
                    start_col=8,
                    end_line=22,
                    end_col=30,
                ),
                Deprecation(
                    code='table-migrate',
                    message='Table numbers is migrated to cata4.counting.numbers in Unity Catalog',
                    start_line=26,
                    start_col=0,
                    end_line=26,
                    end_col=38,
                ),
                Advice(
                    code='table-migrate',
                    message='The default format changed in Databricks Runtime 8.0, from Parquet to Delta',
                    start_line=26,
                    start_col=0,
                    end_line=26,
                    end_col=38,
                ),
            ],
        ),
    ],
)
def test_notebook_linter_tracks_use(extended_test_index, lang, source, expected):
    linter = NotebookLinter.from_source(extended_test_index, source, lang)
    assert linter is not None
    advices = list(linter.lint())
    assert advices == expected
