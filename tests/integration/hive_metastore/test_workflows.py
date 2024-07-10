from datetime import timedelta

import pytest
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.retries import retried


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=10))
@pytest.mark.parametrize(
    "prepare_tables_for_migration,workflow",
    [
        ("regular", "migrate-tables"),
        ("hiveserde", "migrate-external-hiveserde-tables-in-place-experimental"),
        ("hiveserde", "migrate-external-tables-ctas"),
    ],
    indirect=("prepare_tables_for_migration",),
)
def test_table_migration_job_refreshes_migration_status(
    ws,
    installation_ctx,
    prepare_tables_for_migration,
    workflow,
):
    """The migration status should be refreshed after the migration job."""
    tables, _ = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )

    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow(workflow)

    # Avoiding MigrationStatusRefresh as it will refresh the status before fetching
    migration_status_query = f"SELECT * FROM {ctx.config.inventory_database}.migration_status"
    migration_statuses = list(ctx.sql_backend.fetch(migration_status_query))

    if len(migration_statuses) == 0:
        ctx.deployed_workflows.relay_logs(workflow)
        assert False, "No migration statuses found"

    asserts = []
    for table in tables.values():
        migration_status = []
        for status in migration_statuses:
            if status.src_schema == table.schema_name and status.src_table == table.name:
                migration_status.append(status)

        assert_message_postfix = f" found for {table.table_type} {table.full_name}"
        if len(migration_status) == 0:
            asserts.append("No migration status" + assert_message_postfix)
        elif len(migration_status) > 1:
            asserts.append("Multiple migration statuses" + assert_message_postfix)
        elif migration_status[0].dst_schema is None:
            asserts.append("No destination schema" + assert_message_postfix)
        elif migration_status[0].dst_table is None:
            asserts.append("No destination table" + assert_message_postfix)

    assert_message = (
        "\n".join(asserts) + " given migration statuses " + "\n".join([str(status) for status in migration_statuses])
    )
    assert len(asserts) == 0, assert_message


@retried(on=[NotFound], timeout=timedelta(minutes=8))
@pytest.mark.parametrize('prepare_tables_for_migration', [('hiveserde')], indirect=True)
def test_hiveserde_table_in_place_migration_job(ws, installation_ctx, prepare_tables_for_migration):
    tables, dst_schema = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow("migrate-external-hiveserde-tables-in-place-experimental")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-external-hiveserde-tables-in-place-experimental")
    # assert the tables are migrated
    for table in tables.values():
        try:
            assert ws.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"


@retried(on=[NotFound], timeout=timedelta(minutes=8))
@pytest.mark.parametrize('prepare_tables_for_migration', [('hiveserde')], indirect=True)
def test_hiveserde_table_ctas_migration_job(ws, installation_ctx, prepare_tables_for_migration):
    tables, dst_schema = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow("migrate-external-tables-ctas")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-external-tables-ctas")
    # assert the tables are migrated
    for table in tables.values():
        try:
            assert ws.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"
