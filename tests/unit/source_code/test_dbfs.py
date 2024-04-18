import pytest

from databricks.labs.ucx.source_code.base import Deprecation, Advisory
from databricks.labs.ucx.source_code.dbfs import DBFSUsageLinter, FromDbfsFolder


class TestDetectDBFS:
    @pytest.mark.parametrize(
        "code, expected",
        [
            ('"/dbfs/mnt"', 1),
            ('"not a file system path"', 0),
            ('"/dbfs/mnt", "dbfs:/", "/mnt/"', 3),
            ('# "/dbfs/mnt"', 0),
            ('SOME_CONSTANT = "/dbfs/mnt"', 1),
            ('SOME_CONSTANT = "/dbfs/mnt"; load_data(SOME_CONSTANT)', 1),
            ('SOME_CONSTANT = 42; load_data(SOME_CONSTANT)', 0),
        ],
    )
    def test_detects_dbfs_str_const_paths(self, code, expected):
        finder = DBFSUsageLinter()
        advices = finder.lint(code)
        count = 0
        for advice in advices:
            assert isinstance(advice, Advisory)
            count += 1
        assert count == expected

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
            # Would need a stateful linter to detect this next one
            ('DBFS="dbfs:/mnt/foo/bar"; spark.read.parquet(DBFS)', 0),
        ],
    )
    def test_dbfs_usage_linter(self, code, expected):
        linter = DBFSUsageLinter()
        advices = linter.lint(code)
        count = 0
        for advice in advices:
            if isinstance(advice, Deprecation):
                count += 1
        assert count == expected

    def test_dbfs_name(self):
        linter = DBFSUsageLinter()
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
    ftf = FromDbfsFolder()
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
    ftf = FromDbfsFolder()
    assert [
        Deprecation(
            code='dbfs-query',
            message=f'The use of DBFS is deprecated: {table}',
            location_type="<MISSING TYPE>",
            location_path="<MISSING LOCATION>",
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
    ] == list(ftf.lint(query))


def test_dbfs_queries_name():
    ftf = FromDbfsFolder()
    assert ftf.name() == 'dbfs-query'
