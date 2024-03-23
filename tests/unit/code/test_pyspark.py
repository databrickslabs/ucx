from databricks.labs.ucx.code.base import Deprecation
from databricks.labs.ucx.code.pyspark import SparkSql
from databricks.labs.ucx.code.queries import FromTable


def test_spark_not_sql(empty_index):
    ftf = FromTable(empty_index)
    sqf = SparkSql(ftf)

    assert not list(sqf.lint("print(1)"))


def test_spark_sql_no_match(empty_index):
    ftf = FromTable(empty_index)
    sqf = SparkSql(ftf)

    old_code = """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""

    assert not list(sqf.lint(old_code))


def test_spark_sql_match(migration_index):
    ftf = FromTable(migration_index)
    sqf = SparkSql(ftf)

    old_code = """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    assert [
        Deprecation(
            code='table-migrate',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=4,
            start_col=13,
            end_line=4,
            end_col=50,
        )
    ] == list(sqf.lint(old_code))


def test_spark_sql_fix(migration_index):
    ftf = FromTable(migration_index)
    sqf = SparkSql(ftf)

    old_code = """spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    fixed_code = sqf.apply(old_code)
    assert (
        fixed_code
        == """spark.read.csv('s3://bucket/path')
for i in range(10):
    result = spark.sql('SELECT * FROM brand.new.stuff').collect()
    print(len(result))"""
    )
