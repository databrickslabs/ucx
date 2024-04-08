from databricks.labs.ucx.hive_metastore.workflows import TableMigration


def test_migrate_external_tables_sync(run_workflow):
    ctx = run_workflow(TableMigration.migrate_external_tables_sync)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_dbfs_root_delta_tables(run_workflow):
    ctx = run_workflow(TableMigration.migrate_dbfs_root_delta_tables)
    ctx.workspace_client.catalogs.list.assert_called_once()
