import pytest

from databricks.labs.ucx.source_code.base import Deprecation, CurrentSessionState, Failure
from databricks.labs.ucx.source_code.linters.from_table import FromTableSqlLinter


def test_not_migrated_tables_trigger_nothing(empty_index) -> None:
    ftf = FromTableSqlLinter(empty_index, CurrentSessionState())

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    actual = list(ftf.lint(old_query))
    assert not actual


def test_migrated_tables_trigger_messages(migration_index) -> None:
    ftf = FromTableSqlLinter(migration_index, CurrentSessionState())

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"

    assert [
        Deprecation(
            code='table-migrated-to-uc-sql',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
        Deprecation(
            code='table-migrated-to-uc-sql',
            message='Table other.matters is migrated to some.certain.issues in Unity Catalog',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
    ] == list(ftf.lint(old_query))


def test_fully_migrated_queries_match(migration_index) -> None:
    ftf = FromTableSqlLinter(migration_index, CurrentSessionState())

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    new_query = "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"

    assert ftf.apply(old_query) == new_query


def test_fully_migrated_queries_match_no_db(migration_index) -> None:
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)

    old_query = "SELECT * FROM things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    new_query = "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"

    assert ftf.apply(old_query) == new_query


def test_use_database_change(migration_index) -> None:
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)
    query = """
    USE newcatalog;
    SELECT * FROM things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1
    LIMIT 10"""
    _ = list(ftf.lint(query))
    assert ftf.schema == "newcatalog"


def test_use_database_stops_migration(migration_index) -> None:
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)
    query = "SELECT * FROM things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    old_query = f"{query}; USE newcatalog; {query}"
    new_query = (
        "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10; "
        "USE newcatalog; "
        "SELECT * FROM things LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"
    )
    transformed_query = ftf.apply(old_query)
    assert transformed_query == new_query


def test_parses_create_schema(migration_index) -> None:
    query = "CREATE SCHEMA xyz"
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)
    advices = ftf.lint(query)
    assert not list(advices)


def test_parses_drop_schema(migration_index) -> None:
    query = "DROP SCHEMA xyz"
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)
    advices = ftf.lint(query)
    assert not list(advices)


def test_raises_advice_when_parsing_unsupported_sql(migration_index) -> None:
    query = "XDESCRIBE DETAILS xyz"  # not a valid query
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)
    advices = list(ftf.lint(query))
    assert isinstance(advices[0], Failure)
    assert 'not supported' in advices[0].message


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT * FROM hive_metastore.old.things", [("hive_metastore", "old", "things")]),
        ("SELECT * FROM old.things", [("hive_metastore", "old", "things")]),
        ("SELECT * FROM new.things", [("hive_metastore", "new", "things")]),
        ("SELECT * FROM brand.new.things", []),
        ("SELECT * FROM parquet.`dbfs://mnt/foo2/bar2`", []),
    ],
)
def test_linter_collects_tables(query, expected, migration_index) -> None:
    session_state = CurrentSessionState(schema="old")
    ftf = FromTableSqlLinter(migration_index, session_state=session_state)
    tuples = list((info.catalog_name, info.schema_name, info.table_name) for info in ftf.collect_tables(query))
    assert tuples == expected
