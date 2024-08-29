import pytest

from databricks.labs.ucx.source_code.base import Deprecation, Advice, CurrentSessionState, Failure
from databricks.labs.ucx.source_code.linters.dbfs import DBFSUsagePyLinter, DbfsUsageSQLLinter


class TestDetectDBFS:
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
    def test_detects_dbfs_paths(self, code, expected):
        linter = DBFSUsagePyLinter(CurrentSessionState())
        advices = list(linter.lint(code))
        for advice in advices:
            assert isinstance(advice, Advice)
        assert len(advices) == expected

    @pytest.mark.parametrize(
        "code, expected",
        [
            ("load_data('/dbfs/mnt/data')", 1),
            ("load_data('/data')", 0),
            ("load_data('/dbfs/mnt/data', '/data')", 1),
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
    def test_dbfs_usage_linter(self, code, expected):
        linter = DBFSUsagePyLinter(CurrentSessionState())
        advices = linter.lint(code)
        count = 0
        for advice in advices:
            if isinstance(advice, Deprecation):
                count += 1
        assert count == expected

    def test_dbfs_name(self):
        linter = DBFSUsagePyLinter(CurrentSessionState())
        assert linter.name() == "dbfs-usage"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10",
        "SELECT * FROM json.`s3a://abc/d/e/f`",
        "SELECT * FROM delta.`s3a://abc/d/e/f` WHERE foo > 6",
        "SELECT * FROM delta.`s3a://foo/bar`",
        # Make sure non-sql doesn't just fail
        "print('hello')",
        "",
    ],
)
def test_non_dbfs_trigger_nothing(query):
    ftf = DbfsUsageSQLLinter()
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
    ],
)
def test_dbfs_tables_trigger_messages_param(query: str, table: str):
    ftf = DbfsUsageSQLLinter()
    actual = list(ftf.lint(query))
    assert actual == [
        Deprecation(
            code='dbfs-read-from-sql-query',
            message=f'The use of DBFS is deprecated: {table}',
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
def test_dbfs_queries_failure(query: str):
    ftf = DbfsUsageSQLLinter()
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


def test_dbfs_queries_name():
    ftf = DbfsUsageSQLLinter()
    assert ftf.name() == 'dbfs-query'
