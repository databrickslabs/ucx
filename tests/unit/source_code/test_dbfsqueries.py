import pytest
from databricks.labs.ucx.source_code.base import Deprecation
from databricks.labs.ucx.source_code.dbfsqueries import FromDbfsFolder


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10",
        "SELECT * FROM json.'s3a://abc/d/e/f'",
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
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
    ] == list(ftf.lint(query))


def test_dbfs_queries_name():
    ftf = FromDbfsFolder()
    assert ftf.name() == 'dbfs-query'
