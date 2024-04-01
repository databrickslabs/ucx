from databricks.labs.ucx.source_code.base import Deprecation
from databricks.labs.ucx.source_code.pyspark import SparkSql, SPARK_MATCHERS
from databricks.labs.ucx.source_code.queries import FromTable


def test_spark_no_sql(empty_index):
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


def test_spark_table_match(migration_index):
    # import SPARK_MATCHERS from
    for method_name in ["cacheTable", "createTable", "createExternalTable", "getTable", "isCached",
                        "listColumns", "tableExists", "recoverPartitions", "refreshTable", "uncacheTable",
                        "table", "insertInto", "saveAsTable", "register" ]:
        ftf = FromTable(migration_index)
        sqf = SparkSql(ftf)
        matcher = SPARK_MATCHERS[method_name]
        args_list = ["a"] * min(5, matcher.max_args)
        args_list[matcher.table_arg_index] = '"old.things"'
        args = ",".join(args_list)
        old_code = f"""
spark.read.csv("s3://bucket/path")
for i in range(10):
    df = spark.{method_name}({args})
    do_stuff_with_df(df)
"""
        assert [
            Deprecation(
                code='table-migrate',
                message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
                start_line=4,
                start_col=9,
                end_line=4,
                end_col=17+len(method_name)+len(args),
            )
        ] == list(sqf.lint(old_code))


def test_spark_table_no_match(migration_index):
    for method_name in ["cacheTable", "createTable", "createExternalTable", "getTable", "isCached",
                        "listColumns", "tableExists", "recoverPartitions", "refreshTable", "uncacheTable",
                        "table", "insertInto", "saveAsTable", "register" ]:
        ftf = FromTable(migration_index)
        sqf = SparkSql(ftf)
        matcher = SPARK_MATCHERS[method_name]
        args_list = ["a"] * min(5, matcher.max_args)
        args_list[matcher.table_arg_index] = '"table.we.know.nothing.about"'
        args = ",".join(args_list)
        old_code = f"""
spark.read.csv("s3://bucket/path")
for i in range(10):
    df = spark.{method_name}({args})
    do_stuff_with_df(df)
"""
        assert [] == list(sqf.lint(old_code))


def test_spark_table_too_many_args(migration_index):
    for method_name in ["cacheTable", "createTable", "createExternalTable", "getTable", "isCached",
                        "listColumns", "tableExists", "recoverPartitions", "refreshTable", "uncacheTable",
                        "table", "insertInto", "saveAsTable", "register"]:
        ftf = FromTable(migration_index)
        sqf = SparkSql(ftf)
        matcher = SPARK_MATCHERS[method_name]
        if matcher.max_args > 100:
            continue
        args_list = ["a"] * (matcher.max_args + 1)
        args_list[matcher.table_arg_index] = '"table.we.know.nothing.about"'
        args = ",".join(args_list)
        old_code = f"""
spark.read.csv("s3://bucket/path")
for i in range(10):
    df = spark.{method_name}({args})
    do_stuff_with_df(df)
"""
        assert [] == list(sqf.lint(old_code))


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
