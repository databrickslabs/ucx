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
    tables, _ = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        skip_dashboards=False,
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
            f"WHERE src_schema = '{table.schema_name}' AND src_table = '{table.name}'"
        )
        migration_status = list(ctx.sql_backend.fetch(query_migration_status))
        assert_message_postfix = f" found for {table.table_type} {table.full_name}"
        assert len(migration_status) == 1, f"No migration status found" + assert_message_postfix
        assert migration_status[0].dst_schema is not None, f"No destination schema" + assert_message_postfix
        assert migration_status[0].dst_table is not None, f"No destination table" + assert_message_postfix
