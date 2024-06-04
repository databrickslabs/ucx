import ast

import pytest

from databricks.labs.ucx.source_code.base import Deprecation, CurrentSessionState
from databricks.labs.ucx.source_code.linters.pyspark import SparkSql, AstHelper, TableNameMatcher
from databricks.labs.ucx.source_code.queries import FromTable


def test_spark_no_sql(empty_index):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)

    assert not list(sqf.lint("print(1)"))


def test_spark_sql_no_match(empty_index):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)

    old_code = """
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""

    assert not list(sqf.lint(old_code))


def test_spark_sql_match(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)

    old_code = """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    assert [
        Deprecation(
            code='direct-filesystem-access',
            message='The use of direct filesystem references is deprecated: ' 's3://bucket/path',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=34,
        ),
        Deprecation(
            code='table-migrate',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=4,
            start_col=13,
            end_line=4,
            end_col=50,
        ),
    ] == list(sqf.lint(old_code))


def test_spark_sql_match_named(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)

    old_code = """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql(args=[1], sqlQuery = "SELECT * FROM old.things").collect()
    print(len(result))
"""
    assert [
        Deprecation(
            code='direct-filesystem-access',
            message='The use of direct filesystem references is deprecated: ' 's3://bucket/path',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=34,
        ),
        Deprecation(
            code='table-migrate',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=4,
            start_col=13,
            end_line=4,
            end_col=71,
        ),
    ] == list(sqf.lint(old_code))


def test_spark_table_return_value_apply(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)
    old_code = """spark.read.csv('s3://bucket/path')
for table in spark.catalog.listTables():
    do_stuff_with_table(table)"""
    fixed_code = sqf.apply(old_code)
    # no transformations to apply, only lint messages
    assert fixed_code == old_code


def test_spark_sql_fix(migration_index):
    ftf = FromTable(migration_index, CurrentSessionState())
    sqf = SparkSql(ftf, migration_index)

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


@pytest.mark.parametrize(
    "code, expected",
    [
        # Test for 'ls' function
        (
            """dbutils.fs.ls("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=34,
                )
            ],
        ),
        # Test for 'cp' function. Note that the current code will stop at the first deprecation found.
        (
            """dbutils.fs.cp("s3n://bucket/path", "s3n://another_bucket/another_path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3n://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=71,
                )
            ],
        ),
        # Test for 'rm' function
        (
            """dbutils.fs.rm("s3://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=33,
                )
            ],
        ),
        # Test for 'head' function
        (
            """dbutils.fs.head("wasb://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: wasb://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=37,
                )
            ],
        ),
        # Test for 'put' function
        (
            """dbutils.fs.put("wasbs://bucket/path", "data")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: wasbs://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=45,
                )
            ],
        ),
        # Test for 'mkdirs' function
        (
            """dbutils.fs.mkdirs("abfs://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: abfs://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=39,
                )
            ],
        ),
        # Test for 'move' function
        (
            """dbutils.fs.mv("wasb://bucket/path", "wasb://another_bucket/another_path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: wasb://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=73,
                )
            ],
        ),
        # Test for 'text' function
        (
            """spark.read.text("wasbs://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: wasbs://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=38,
                )
            ],
        ),
        # Test for 'csv' function
        (
            """spark.read.csv("abfs://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: abfs://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=36,
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
                    code='direct-filesystem-access',
                    message='The use of direct filesystem references is deprecated: '
                    "s3a://your_bucket_name/your_directory/",
                    start_line=1,
                    start_col=1,
                    end_line=3,
                    end_col=59,
                )
            ],
        ),
        # Test for 'json' function
        (
            """spark.read.json("abfss://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: abfss://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=38,
                )
            ],
        ),
        # Test for 'orc' function
        (
            """spark.read.orc("dbfs://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: dbfs://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=36,
                )
            ],
        ),
        # Test for 'parquet' function
        (
            """spark.read.parquet("hdfs://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: hdfs://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=40,
                )
            ],
        ),
        # Test for 'save' function
        (
            """spark.write.save("file://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: file://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=38,
                )
            ],
        ),
        # Test for 'load' function with default to dbfs
        (
            """spark.read.load("/bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of default dbfs: references is deprecated: /bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=31,
                )
            ],
        ),
        # Test for 'addFile' function
        (
            """spark.addFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
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
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
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
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
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
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=40,
                )
            ],
        ),
        # Test for 'hadoopFile' function
        (
            """spark.hadoopFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=37,
                )
            ],
        ),
        # Test for 'newAPIHadoopFile' function
        (
            """spark.newAPIHadoopFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=43,
                )
            ],
        ),
        # Test for 'pickleFile' function
        (
            """spark.pickleFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=37,
                )
            ],
        ),
        # Test for 'saveAsHadoopFile' function
        (
            """spark.saveAsHadoopFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=43,
                )
            ],
        ),
        # Test for 'saveAsNewAPIHadoopFile' function
        (
            """spark.saveAsNewAPIHadoopFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=49,
                )
            ],
        ),
        # Test for 'saveAsPickleFile' function
        (
            """spark.saveAsPickleFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=43,
                )
            ],
        ),
        # Test for 'saveAsSequenceFile' function
        (
            """spark.saveAsSequenceFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=45,
                )
            ],
        ),
        # Test for 'saveAsTextFile' function
        (
            """spark.saveAsTextFile("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=41,
                )
            ],
        ),
        # Test for 'load_from_path' function
        (
            """spark.load_from_path("s3a://bucket/path")""",
            [
                Deprecation(
                    code='direct-filesystem-access',
                    message="The use of direct filesystem references is deprecated: s3a://bucket/path",
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=41,
                )
            ],
        ),
    ],
)
def test_spark_cloud_direct_access(empty_index, code, expected):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)
    advisories = list(sqf.lint(code))
    assert advisories == expected


FS_FUNCTIONS = [
    "ls",
    "cp",
    "rm",
    "mv",
    "head",
    "put",
    "mkdirs",
]


@pytest.mark.parametrize("fs_function", FS_FUNCTIONS)
def test_direct_cloud_access_reports_nothing(empty_index, fs_function):
    ftf = FromTable(empty_index, CurrentSessionState())
    sqf = SparkSql(ftf, empty_index)
    # ls function calls have to be from dbutils.fs, or we ignore them
    code = f"""spark.{fs_function}("/bucket/path")"""
    advisories = list(sqf.lint(code))
    assert not advisories


def test_get_full_function_name():

    # Test when node.func is an instance of ast.Attribute
    node = ast.Call(func=ast.Attribute(value=ast.Name(id='value'), attr='attr'))
    # noinspection PyProtectedMember
    assert AstHelper.get_full_function_name(node) == 'value.attr'

    # Test when node.func is an instance of ast.Name
    node = ast.Call(func=ast.Name(id='name'))
    # noinspection PyProtectedMember
    assert AstHelper.get_full_function_name(node) == 'name'

    # Test when node.func is neither ast.Attribute nor ast.Name
    node = ast.Call(func=ast.Constant(value='constant'))
    # noinspection PyProtectedMember
    assert AstHelper.get_full_function_name(node) is None

    # Test when next_node in _get_value is an instance of ast.Name
    node = ast.Call(func=ast.Attribute(value=ast.Name(id='name'), attr='attr'))
    # noinspection PyProtectedMember
    assert AstHelper.get_full_function_name(node) == 'name.attr'

    # Test when next_node in _get_value is an instance of ast.Attribute
    node = ast.Call(func=ast.Attribute(value=ast.Attribute(value=ast.Name(id='value'), attr='attr'), attr='attr'))
    # noinspection PyProtectedMember
    assert AstHelper.get_full_function_name(node) == 'value.attr.attr'

    # Test when next_node in _get_value is neither ast.Name nor ast.Attribute
    node = ast.Call(func=ast.Attribute(value=ast.Constant(value='constant'), attr='attr'))
    # noinspection PyProtectedMember
    assert AstHelper.get_full_function_name(node) is None


def test_apply_table_name_matcher(migration_index):
    from_table = FromTable(migration_index, CurrentSessionState('old'))
    matcher = TableNameMatcher('things', 1, 1, 0)

    # Test when table_arg is an instance of ast.Constant but the destination does not exist in the index
    node = ast.Call(args=[ast.Constant(value='some.things')])
    matcher.apply(from_table, migration_index, node)
    table_constant = node.args[0]
    assert isinstance(table_constant, ast.Constant)
    assert table_constant.value == 'some.things'

    # Test when table_arg is an instance of ast.Constant and the destination exists in the index
    node = ast.Call(args=[ast.Constant(value='old.things')])
    matcher.apply(from_table, migration_index, node)
    table_constant = node.args[0]
    assert isinstance(table_constant, ast.Constant)
    assert table_constant.value == 'brand.new.stuff'
