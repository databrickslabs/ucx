from typing import cast

import pytest

from astroid import Call, Const, Expr  # type: ignore

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import Deprecation, CurrentSessionState, SqlLinter
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.python.python_ast import MaybeTree, TreeHelper
from databricks.labs.ucx.source_code.linters.pyspark import SparkSqlPyLinter
from databricks.labs.ucx.source_code.linters.from_table import FromTableSqlLinter
from databricks.labs.ucx.source_code.linters.pyspark import SparkCallMatcher, SparkTableNamePyLinter


def test_spark_no_sql(empty_index) -> None:
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(empty_index, session_state)
    sqf = SparkTableNamePyLinter(ftf, empty_index, session_state)

    assert not list(sqf.lint("print(1)"))


def test_spark_dynamic_sql(empty_index) -> None:
    source = """
schema="some_schema"
df4.write.saveAsTable(f"{schema}.member_measure")
"""
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(empty_index, session_state)
    sqf = SparkTableNamePyLinter(ftf, empty_index, session_state)
    assert not list(sqf.lint(source))


def test_spark_sql_no_match(empty_index) -> None:
    context = LinterContext(empty_index, CurrentSessionState())
    sql_linter = context.linter(Language.SQL)
    spark_linter = SparkSqlPyLinter(cast(SqlLinter, sql_linter), None)

    old_code = """
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""

    assert not list(spark_linter.lint(old_code))


def test_spark_sql_match(migration_index) -> None:
    context = LinterContext(migration_index, CurrentSessionState())
    sql_linter = context.linter(Language.SQL)
    spark_linter = SparkSqlPyLinter(cast(SqlLinter, sql_linter), None)
    python_code = """
spark.sql("SELECT * FROM old.things")
spark.sql("SELECT * FROM csv.`s3://bucket/path`")
"""
    advices = list(spark_linter.lint(python_code))
    assert [
        Deprecation(
            code='table-migrated-to-uc-sql',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=1,
            start_col=0,
            end_line=1,
            end_col=37,
        ),
        Deprecation(
            code='direct-filesystem-access-in-sql-query',
            message='The use of direct filesystem references is deprecated: s3://bucket/path',
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=49,
        ),
    ] == advices


def test_spark_sql_match_named(migration_index) -> None:
    context = LinterContext(migration_index, CurrentSessionState())
    sql_linter = context.linter(Language.SQL)
    spark_linter = SparkSqlPyLinter(cast(SqlLinter, sql_linter), None)
    old_code = """
spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql(args=[1], sqlQuery = "SELECT * FROM old.things").collect()
    print(len(result))
"""
    assert list(spark_linter.lint(old_code)) == [
        Deprecation(
            code='table-migrated-to-uc-sql',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=3,
            start_col=13,
            end_line=3,
            end_col=71,
        ),
    ]


def test_spark_table_return_value_apply(migration_index) -> None:
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(migration_index, session_state)
    sqf = SparkTableNamePyLinter(ftf, migration_index, session_state)
    old_code = """spark.read.csv('s3://bucket/path')
for table in spark.catalog.listTables():
    do_stuff_with_table(table)"""
    fixed_code = sqf.apply(old_code)
    # no transformations to apply, only lint messages
    assert fixed_code.rstrip() == old_code.rstrip()


def test_spark_sql_tablename_fix(migration_index) -> None:
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(migration_index, session_state)
    spark_linter = SparkSqlPyLinter(ftf, ftf)

    old_code = """spark.read.csv("s3://bucket/path")
for i in range(10):
    result = spark.sql("SELECT * FROM old.things").collect()
    print(len(result))
"""
    fixed_code = spark_linter.apply(old_code)
    assert (
        fixed_code.rstrip()
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=36,
                )
            ],
        ),
        # Test for option function
        (
            """df=spark.sql('SELECT * FROm dual')
(df.write
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    message="The use of direct filesystem references is deprecated: /bucket/path",
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=41,
                )
            ],
        ),
    ],
)
def test_spark_cloud_direct_access(empty_index, code, expected) -> None:
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(empty_index, session_state)
    sqf = SparkTableNamePyLinter(ftf, empty_index, session_state)
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
def test_direct_cloud_access_to_workspace_reports_nothing(empty_index, fs_function) -> None:
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(empty_index, session_state)
    sqf = SparkTableNamePyLinter(ftf, empty_index, session_state)
    # ls function calls have to be from dbutils.fs, or we ignore them
    code = f"""spark.{fs_function}("/Workspace/bucket/path")"""
    advisories = list(sqf.lint(code))
    assert not advisories


@pytest.mark.parametrize("fs_function", FS_FUNCTIONS)
def test_direct_cloud_access_to_volumes_reports_nothing(empty_index, fs_function) -> None:
    session_state = CurrentSessionState()
    ftf = FromTableSqlLinter(empty_index, session_state)
    sqf = SparkTableNamePyLinter(ftf, empty_index, session_state)
    # ls function calls have to be from dbutils.fs, or we ignore them
    code = f"""spark.{fs_function}("/Volumes/bucket/path")"""
    advisories = list(sqf.lint(code))
    assert not advisories


def test_get_full_function_name_for_member_function() -> None:
    maybe_tree = MaybeTree.from_source_code("value.attr()")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert isinstance(node.value, Call)
    assert TreeHelper.get_full_function_name(node.value) == 'value.attr'


def test_get_full_function_name_for_member_member_function() -> None:
    maybe_tree = MaybeTree.from_source_code("value1.value2.attr()")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert isinstance(node.value, Call)
    assert TreeHelper.get_full_function_name(node.value) == 'value1.value2.attr'


def test_get_full_function_name_for_chained_function() -> None:
    maybe_tree = MaybeTree.from_source_code("value.attr1().attr2()")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert isinstance(node.value, Call)
    assert TreeHelper.get_full_function_name(node.value) == 'value.attr1.attr2'


def test_get_full_function_name_for_global_function() -> None:
    maybe_tree = MaybeTree.from_source_code("name()")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert isinstance(node.value, Call)
    assert TreeHelper.get_full_function_name(node.value) == 'name'


def test_get_full_function_name_for_non_method() -> None:
    maybe_tree = MaybeTree.from_source_code("not_a_function")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert TreeHelper.get_full_function_name(node.value) is None


def test_apply_table_name_matcher_with_missing_constant(migration_index) -> None:
    from_table = FromTableSqlLinter(migration_index, CurrentSessionState('old'))
    matcher = SparkCallMatcher('things', 1, 1, 0)
    maybe_tree = MaybeTree.from_source_code("call('some.things')")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert isinstance(node.value, Call)
    matcher.apply(from_table, migration_index, node.value)
    table_constant = node.value.args[0]
    assert isinstance(table_constant, Const)
    assert table_constant.value == 'some.things'


def test_apply_table_name_matcher_with_existing_constant(migration_index) -> None:
    from_table = FromTableSqlLinter(migration_index, CurrentSessionState('old'))
    matcher = SparkCallMatcher('things', 1, 1, 0)
    maybe_tree = MaybeTree.from_source_code("call('old.things')")
    assert maybe_tree.tree is not None
    node = maybe_tree.tree.first_statement()
    assert isinstance(node, Expr)
    assert isinstance(node.value, Call)
    matcher.apply(from_table, migration_index, node.value)
    table_constant = node.value.args[0]
    assert isinstance(table_constant, Const)
    assert table_constant.value == 'brand.new.stuff'


@pytest.mark.parametrize(
    "source_code, expected",
    [
        ("spark.table('my_schema.my_table')", [('hive_metastore', 'my_schema', 'my_table')]),
        ("spark.read.parquet('dbfs://mnt/foo2/bar2')", []),
    ],
)
def test_spark_collect_tables_ignores_dfsas(source_code, expected, migration_index) -> None:
    session_state = CurrentSessionState('old')
    from_table = FromTableSqlLinter(migration_index, session_state)
    linter = SparkTableNamePyLinter(from_table, migration_index, session_state)
    used_tables = list(linter.collect_tables(source_code))
    for used_table in used_tables:
        actual = (used_table.catalog_name, used_table.schema_name, used_table.table_name)
        assert actual in expected
