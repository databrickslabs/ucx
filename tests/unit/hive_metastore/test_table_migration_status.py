from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus


def test_table_migration_status_failures_when_pending_migration() -> None:
    migration_status = TableMigrationStatus("schema", "table")
    assert migration_status.failures == ["Object 'schema.table' pending migration"]
