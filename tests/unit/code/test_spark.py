from databricks.labs.ucx.code.pyspark import SparkSql
from databricks.labs.ucx.code.queries import FromTable
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus


def test_spark_not_sql():
    ftf = FromTable(Index([]))

    sqf = SparkSql(ftf)

    assert not sqf.lint("print(1)")


def test_spark_sql_no_match():
    ftf = FromTable(Index([]))

    sqf = SparkSql(ftf)

    assert not sqf.lint(
        """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    )


def test_spark_sql_match():
    ftf = FromTable(Index([MigrationStatus('old', 'things', 'brand', 'new', 'stuff')]))

    sqf = SparkSql(ftf)

    assert list(sqf.lint(
        """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    ))


def test_spark_sql_fix():
    ftf = FromTable(Index([MigrationStatus('old', 'things', 'brand', 'new', 'stuff')]))

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
