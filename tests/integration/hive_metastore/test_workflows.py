from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried


@retried(on=[NotFound], timeout=timedelta(minutes=5))
@pytest.mark.parametrize(
    "prepare_tables_for_migration,workflow",
    [
        ("regular", "migrate-tables"),
        ("hiveserde", "migrate-external-hiveserde-tables-in-place-experimental"),
        ("hiveserde", "migrate-external-tables-ctas"),
    ],
    indirect=("prepare_tables_for_migration",),
)
def test_table_migration_job_refreshes_migration_status(ws, installation_ctx, prepare_tables_for_migration, workflow):
    """The migration status should be refreshed after the migration job."""
    tables, dst_schema = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )

    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow(workflow)

    for table in tables.values():
        # Avoiding MigrationStatusRefresh as it will refresh the status before fetching
        query_migration_status = (
            f"SELECT * FROM {ctx.config.inventory_database}.migration_status "
            f"WHERE src_schema = '{dst_schema.name}' AND src_table = '{table.name}'"
        )
        migration_status = list(ctx.sql_backend.fetch(query_migration_status))
        assert len(migration_status) == 1
