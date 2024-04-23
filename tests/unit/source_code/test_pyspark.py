import pytest

from databricks.labs.ucx.source_code.base import Advisory, Deprecation, CurrentSessionState
from databricks.labs.ucx.source_code.pyspark import SparkMatchers, SparkSql
from databricks.labs.ucx.source_code.queries import FromTable


def test_spark_no_sql(empty_index):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)

    assert not list(sqf.lint("print(1)"))


def test_spark_sql_no_match(empty_index):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)

    old_code = """
spark.read.csv("/bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""

    assert not list(sqf.lint(old_code))


def test_spark_sql_match(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)

    old_code = """
spark.read.csv("/some/path")
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


def test_spark_sql_match_named(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)

    old_code = """
spark.read.csv("/some/path")
for i in range(10):
    result = spark.sql(args=[1], sqlQuery = "SELECT * FROM old.things").collect()
    print(len(result))
"""
    assert [
        Deprecation(
            code='table-migrate',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=4,
            start_col=13,
            end_line=4,
            end_col=71,
        )
    ] == list(sqf.lint(old_code))


METHOD_NAMES = [
    "cacheTable",
    "createTable",
    "createExternalTable",
    "getTable",
    "isCached",
    "listColumns",
    "tableExists",
    "recoverPartitions",
    "refreshTable",
    "uncacheTable",
    "table",
    "insertInto",
    "saveAsTable",
    "register",
]


@pytest.mark.parametrize("method_name", METHOD_NAMES)
def test_spark_table_match(migration_index, method_name):
    spark_matchers = SparkMatchers()
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    matcher = spark_matchers.matchers[method_name]
    args_list = ["a"] * min(5, matcher.max_args)
    args_list[matcher.table_arg_index] = '"old.things"'
    args = ",".join(args_list)
    old_code = f"""
spark.read.csv("/some/path")
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
            end_col=17 + len(method_name) + len(args),
        ),
    ] == list(sqf.lint(old_code))


@pytest.mark.parametrize("method_name", METHOD_NAMES)
def test_spark_table_no_match(migration_index, method_name):
    spark_matchers = SparkMatchers()
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    matcher = spark_matchers.matchers[method_name]
    args_list = ["a"] * min(5, matcher.max_args)
    args_list[matcher.table_arg_index] = '"table.we.know.nothing.about"'
    args = ",".join(args_list)
    old_code = f"""
spark.read.csv("/some/path")
for i in range(10):
    df = spark.{method_name}({args})
    do_stuff_with_df(df)
"""
    assert not list(sqf.lint(old_code))


@pytest.mark.parametrize("method_name", METHOD_NAMES)
def test_spark_table_too_many_args(migration_index, method_name):
    spark_matchers = SparkMatchers()
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    matcher = spark_matchers.matchers[method_name]
    if matcher.max_args > 100:
        return
    args_list = ["a"] * (matcher.max_args + 1)
    args_list[matcher.table_arg_index] = '"table.we.know.nothing.about"'
    args = ",".join(args_list)
    old_code = f"""
spark.read.csv("/some/path")
for i in range(10):
    df = spark.{method_name}({args})
    do_stuff_with_df(df)
"""
    assert not list(sqf.lint(old_code))


def test_spark_table_named_args(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    old_code = """
spark.read.csv("/some/path")
for i in range(10):
    df = spark.saveAsTable(format="xyz", name="old.things")
    do_stuff_with_df(df)
"""
    assert [
        Deprecation(
            code='table-migrate',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=4,
            start_col=9,
            end_line=4,
            end_col=59,
        )
    ] == list(sqf.lint(old_code))


def test_spark_table_variable_arg(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    old_code = """
spark.read.csv("/some/path")
for i in range(10):
    df = spark.saveAsTable(name)
    do_stuff_with_df(df)
"""
    assert [
        Advisory(
            code='table-migrate',
            message="Can't migrate 'saveAsTable' because its table name argument is not a constant",
            start_line=4,
            start_col=9,
            end_line=4,
            end_col=32,
        )
    ] == list(sqf.lint(old_code))


def test_spark_table_fstring_arg(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    old_code = """
spark.read.csv("/some/path")
for i in range(10):
    df = spark.saveAsTable(f"boop{stuff}")
    do_stuff_with_df(df)
"""
    assert [
        Advisory(
            code='table-migrate',
            message="Can't migrate 'saveAsTable' because its table name argument is not a constant",
            start_line=4,
            start_col=9,
            end_line=4,
            end_col=42,
        )
    ] == list(sqf.lint(old_code))


def test_spark_table_return_value(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    old_code = """
spark.read.csv("/some/path")
for table in spark.listTables():
    do_stuff_with_table(table)
"""
    assert [
        Advisory(
            code='table-migrate',
            message="Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.",
            start_line=3,
            start_col=13,
            end_line=3,
            end_col=31,
        )
    ] == list(sqf.lint(old_code))


def test_spark_sql_fix(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)

    old_code = """spark.read.csv("/some/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    fixed_code = sqf.apply(old_code)
    assert (
        fixed_code
        == """spark.read.csv('/some/path')
for i in range(10):
    result = spark.sql('SELECT * FROM brand.new.stuff').collect()
    print(len(result))"""
    )


@pytest.mark.parametrize(
    "code, expected",
    [
        # Test for 'ls' function
        (
            """spark.ls("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=29,
                )
            ],
        ),
        # Test for 'cp' function. Note that the current code will stop at the first deprecation found.
        (
            """spark.cp("s3a://bucket/path", "s3a://another_bucket/another_path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=66,
                )
            ],
        ),
        # Test for 'rm' function
        (
            """spark.rm("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=29,
                )
            ],
        ),
        # Test for 'head' function
        (
            """spark.head("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=31,
                )
            ],
        ),
        # Test for 'put' function
        (
            """spark.put("s3a://bucket/path", "data")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=38,
                )
            ],
        ),
        # Test for 'mkdirs' function
        (
            """spark.mkdirs("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=33,
                )
            ],
        ),
        # Test for 'move' function
        (
            """spark.move("s3a://bucket/path", "s3a://another_bucket/another_path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=68,
                )
            ],
        ),
        # Test for 'text' function
        (
            """spark.read.text("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=36,
                )
            ],
        ),
        # Test for 'csv' function
        (
            """spark.read.csv("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=35,
                )
            ],
        ),
        # Test for option function
        (
            """(df.write
  .format("parquet")
  .option("path", "s3a://your_bucket_name/your_directory/")
  .option("spark.hadoop.fs.s3a.access.key", "your_access_key")
  .option("spark.hadoop.fs.s3a.secret.key", "your_secret_key")
  .save())""",
            [
                Deprecation(
                    code='cloud-access',
                    message='The use of cloud direct references is deprecated: '
                    "'s3a://your_bucket_name/your_directory/'",
                    start_line=1,
                    start_col=1,
                    end_line=3,
                    end_col=59,
                )
            ],
        ),
        # Test for 'json' function
        (
            """spark.read.json("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=36,
                )
            ],
        ),
        # Test for 'orc' function
        (
            """spark.read.orc("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=35,
                )
            ],
        ),
        # Test for 'parquet' function
        (
            """spark.read.parquet("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=39,
                )
            ],
        ),
        # Test for 'save' function
        (
            """spark.write.save("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=37,
                )
            ],
        ),
        # Test for 'load' function
        (
            """spark.read.load("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=36,
                )
            ],
        ),
        # Test for 'addFile' function
        (
            """spark.addFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=34,
                )
            ],
        ),
        # Test for 'binaryFiles' function
        (
            """spark.binaryFiles("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=38,
                )
            ],
        ),
        # Test for 'binaryRecords' function
        (
            """spark.binaryRecords("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=40,
                )
            ],
        ),
        # Test for 'dump_profiles' function
        (
            """spark.dump_profiles("s3a://bucket/path")""",
            [
                Deprecation(
                    code='cloud-access',
                    message="The use of cloud direct references is deprecated: 's3a://bucket/path'",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=40,
                )
            ],
        ),
    ],
)
# TODO Add more tests
def test_spark_cloud_direct_access(empty_index, code, expected):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)
    advisories = list(sqf.lint(code))
    assert advisories == expected
