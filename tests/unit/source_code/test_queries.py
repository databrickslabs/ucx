from databricks.labs.ucx.source_code.base import Deprecation, Advice
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
            source_type=Advice.MISSING_SOURCE_TYPE,
            source_path=Advice.MISSING_SOURCE_PATH,
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        ),
        Deprecation(
            code='table-migrate',
            message='Table other.matters is migrated to some.certain.issues in Unity Catalog',
            source_type=Advice.MISSING_SOURCE_TYPE,
            source_path=Advice.MISSING_SOURCE_PATH,
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
