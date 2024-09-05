import pytest

from databricks.labs.ucx.source_code.base import Deprecation, Advice, CurrentSessionState, Failure
from databricks.labs.ucx.source_code.linters.directfs import (
    DIRECT_FS_ACCESS_PATTERNS,
    DirectFsAccessSqlLinter,
    DirectFsAccessPyLinter,
)


@pytest.mark.parametrize(
    "path, matches",
    [
        ("/mnt/foo/bar", True),
        ("dbfs:/mnt/foo/bar", True),
        ("s3a://bucket1/folder1", True),
        ("/dbfs/mnt/foo/bar", True),
        ("/tmp/foo", True),
        ("table.we.know.nothing.about", False),
    ],
)
def test_matches_dfsa_pattern(path, matches):
    """see https://github.com/databrickslabs/ucx/issues/2350"""
    matched = any(pattern.matches(path) for pattern in DIRECT_FS_ACCESS_PATTERNS)
    assert matches == matched


@pytest.mark.parametrize(
    "code, expected",
    [
        ('SOME_CONSTANT = "not a file system path"', 0),
        ('SOME_CONSTANT = ("/dbfs/mnt", "dbfs:/", "/mnt/")', 3),
        ('# "/dbfs/mnt"', 0),
        ('SOME_CONSTANT = "/dbfs/mnt"', 1),
        ('SOME_CONSTANT = "/dbfs/mnt"; load_data(SOME_CONSTANT)', 1),
        ('SOME_CONSTANT = 42; load_data(SOME_CONSTANT)', 0),
    ],
)
def test_detects_dfsa_paths(code, expected):
    linter = DirectFsAccessPyLinter(CurrentSessionState(), prevent_spark_duplicates=False)
    advices = list(linter.lint(code))
    for advice in advices:
        assert isinstance(advice, Advice)
    assert len(advices) == expected


@pytest.mark.parametrize(
    "code, expected",
    [
        ("load_data('/dbfs/mnt/data')", 1),
        ("load_data('/data')", 1),
        ("load_data('/dbfs/mnt/data', '/data')", 2),
        ("# load_data('/dbfs/mnt/data', '/data')", 0),
        ('spark.read.parquet("/mnt/foo/bar")', 1),
        ('spark.read.parquet("dbfs:/mnt/foo/bar")', 1),
        ('spark.read.parquet("dbfs://mnt/foo/bar")', 1),
        ('DBFS="dbfs:/mnt/foo/bar"; spark.read.parquet(DBFS)', 1),
    ],
)
def test_dfsa_usage_linter(code, expected):
    linter = DirectFsAccessPyLinter(CurrentSessionState(), prevent_spark_duplicates=False)
    advices = linter.lint(code)
    count = 0
    for advice in advices:
        if isinstance(advice, Deprecation):
            count += 1
    assert count == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10",
        # Make sure non-sql doesn't just fail
        "print('hello')",
        "",
    ],
)
def test_non_dfsa_triggers_nothing(query):
    ftf = DirectFsAccessSqlLinter()
    assert not list(ftf.lint(query))


@pytest.mark.parametrize(
    "query, table",
    [
        ('SELECT * FROM parquet.`dbfs:/...` LIMIT 10', "dbfs:/..."),
        ("SELECT * FROM delta.`/mnt/...` WHERE foo > 6", "/mnt/..."),
        ("SELECT * FROM json.`/a/b/c` WHERE foo > 6", "/a/b/c"),
        ("DELETE FROM json.`/...` WHERE foo = 'bar'", "/..."),
        ("MERGE INTO delta.`/dbfs/...` t USING src ON t.key = src.key WHEN MATCHED THEN DELETE", "/dbfs/..."),
        ("SELECT * FROM json.`s3a://abc/d/e/f`", "s3a://abc/d/e/f"),
        ("SELECT * FROM delta.`s3a://abc/d/e/f` WHERE foo > 6", "s3a://abc/d/e/f"),
        ("SELECT * FROM delta.`s3a://foo/bar`", "s3a://foo/bar"),
        ("SELECT * FROM csv.`dbfs:/mnt/foo`", "dbfs:/mnt/foo"),
    ],
)
def test_dfsa_tables_trigger_messages_param(query: str, table: str):
    ftf = DirectFsAccessSqlLinter()
    actual = list(ftf.lint(query))
    assert actual == [
        Deprecation(
            code='direct-filesystem-access-in-sql-query',
            message=f'The use of direct filesystem references is deprecated: {table}',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
    ]


@pytest.mark.parametrize(
    "query",
    [
        'SELECT * FROM {{some_db.some_table}}',
    ],
)
def test_dfsa_queries_failure(query: str):
    ftf = DirectFsAccessSqlLinter()
    actual = list(ftf.lint(query))
    assert actual == [
        Failure(
            code='sql-parse-error',
            message=f'SQL expression is not supported yet: {query}',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
    ]
