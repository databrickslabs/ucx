import pytest

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus


def test_table_migration_status_failures_when_pending_migration() -> None:
    migration_status = TableMigrationStatus("schema", "table")
    assert migration_status.failures == ["Object 'schema.table' pending migration"]


def test_table_migration_status_no_failures_when_destination_is_present() -> None:
    migration_status = TableMigrationStatus("schema", "table", "catalog", "schema", "table")
    assert migration_status.failures == []


@pytest.mark.parametrize("attribute_one", ["dst_catalog", "dst_schema", "dst_table"])
@pytest.mark.parametrize("attribute_two", ["dst_catalog", "dst_schema", "dst_table"])
def test_table_migration_status_failures_when_partial_destination(attribute_one: str, attribute_two: str) -> None:
    migration_status = TableMigrationStatus(
        "schema", "table", **{attribute_one: attribute_one, attribute_two: attribute_two}
    )
    assert migration_status.failures == ["[UCX INTERNAL] Object 'schema.table' has partially destination"]
