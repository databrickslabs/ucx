from databricks.labs.ucx.recon.workflows import MigrationRecon


def test_migration_recon_refresh(run_workflow):
    ctx = run_workflow(MigrationRecon.migration_recon_refresh)
    ctx.workspace_client.catalogs.list.assert_called_once()
