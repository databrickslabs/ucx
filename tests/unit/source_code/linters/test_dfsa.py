import pytest

from databricks.labs.ucx.source_code.base import Deprecation, Advice, CurrentSessionState, Failure
from databricks.labs.ucx.source_code.linters.dfsa import DFSAPyLinter, DFSASQLLinter, DFSA_PATTERNS


"""see https://github.com/databrickslabs/ucx/issues/2350"""
@pytest.mark.parametrize(
    "path, matches", [
        ("/mnt/foo/bar",True),
        ("dbfs:/mnt/foo/bar",True),
        ("s3a://bucket1/folder1", True),
        ("/dbfs/mnt/foo/bar",True),
        ("/tmp/foo", False),
    ])
def test_matches_dfs_pattern(path, matches):
    matched = any(pattern.matches(path) for pattern in DFSA_PATTERNS)
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
    linter = DFSAPyLinter(CurrentSessionState(), allow_spark_duplicates=True)
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
        (
            """
DBFS1="dbfs:/mnt/foo/bar1"
systems=[DBFS1, "dbfs:/mnt/foo/bar2"]
for system in systems:
    spark.read.parquet(system)
""",
            2,
        ),
    ],
)
def test_dfsa_usage_linter(code, expected):
    linter = DFSAPyLinter(CurrentSessionState(), allow_spark_duplicates=True)
    advices = linter.lint(code)
    count = 0
    for advice in advices:
        if isinstance(advice, Deprecation):
            count += 1
    assert count == expected


def test_dfsa_name():
    linter = DFSAPyLinter(CurrentSessionState())
    assert linter.name() == "dfsa-usage"


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
    ftf = DFSASQLLinter()
    assert not list(ftf.lint(query))


@pytest.mark.parametrize(
    "query, table",
    [
        ('SELECT * FROM parquet.`dbfs:/...` LIMIT 10', "dbfs:/..."),
        ("SELECT * FROM delta.`/mnt/...` WHERE foo > 6", "/mnt/..."),
        ("SELECT * FROM json.`/a/b/c` WHERE foo > 6", "/a/b/c"),
        ("DELETE FROM json.`/...` WHERE foo = 'bar'", "/..."),
        (
            "MERGE INTO delta.`/dbfs/...` t USING source ON t.key = source.key WHEN MATCHED THEN DELETE",
            "/dbfs/...",
        ),
        ("SELECT * FROM json.`s3a://abc/d/e/f`", "s3a://abc/d/e/f"),
        ("SELECT * FROM delta.`s3a://abc/d/e/f` WHERE foo > 6", "s3a://abc/d/e/f"),
        ("SELECT * FROM delta.`s3a://foo/bar`", "s3a://foo/bar"),
    ],
)
def test_dfsa_tables_trigger_messages_param(query: str, table: str):
    ftf = DFSASQLLinter()
    actual = list(ftf.lint(query))
    assert actual == [
        Deprecation(
            code='direct-file-system-access-in-sql-query',
            message=f'The use of direct file system access is deprecated: {table}',
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
    ftf = DFSASQLLinter()
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


def test_dfsa_queries_name():
    ftf = DFSASQLLinter()
    assert ftf.name() == 'dfsa-query'
