from databricks.labs.ucx.source_code.base import Deprecation
from databricks.labs.ucx.source_code.queries import FromTable


def test_not_migrated_tables_trigger_nothing(empty_index):
    ftf = FromTable(empty_index)

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"

    assert not list(ftf.lint(old_query))


def test_migrated_tables_trigger_messages(migration_index):
    ftf = FromTable(migration_index)

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"

    assert [
        Deprecation(
            code='table-migrate',
            message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
        Deprecation(
            code='table-migrate',
            message='Table other.matters is migrated to some.certain.issues in Unity Catalog',
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
    ] == list(ftf.lint(old_query))


def test_fully_migrated_queries_match(migration_index):
    ftf = FromTable(migration_index)

    old_query = "SELECT * FROM old.things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    new_query = "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"

    assert ftf.apply(old_query) == new_query


def test_fully_migrated_queries_match_no_db(migration_index):
    ftf = FromTable(migration_index, use_schema="old")

    old_query = "SELECT * FROM things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    new_query = "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"

    assert ftf.apply(old_query) == new_query


def test_use_database_change(migration_index):
    ftf = FromTable(migration_index, use_schema="old")
    query = """
    USE newcatalog;  
    SELECT * FROM things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 
    LIMIT 10"""
    _ = list(ftf.lint(query))
    assert ftf.schema == "newcatalog"


def test_use_database_stops_migration(migration_index):
    ftf = FromTable(migration_index, use_schema="old")
    query = "SELECT * FROM things LEFT JOIN hive_metastore.other.matters USING (x) WHERE state > 1 LIMIT 10"
    old_query = f"{query}; USE newcatalog; {query}"
    new_query = (
        "SELECT * FROM brand.new.stuff LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10; "
        "USE newcatalog; "
        "SELECT * FROM things LEFT JOIN some.certain.issues USING (x) WHERE state > 1 LIMIT 10"
    )
    transformed_query = ftf.apply(old_query)
    assert transformed_query == new_query
