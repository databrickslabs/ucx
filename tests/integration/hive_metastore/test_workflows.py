import dataclasses
from typing import Literal

import pytest
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table


@pytest.mark.parametrize(
    "scenario, workflow",
    [
        ("regular", "migrate-tables"),
        ("hiveserde", "migrate-external-hiveserde-tables-in-place-experimental"),
        ("hiveserde", "migrate-external-tables-ctas"),
    ],
)
def test_table_migration_job_refreshes_migration_status(
    installation_ctx,
    scenario: Literal["regular", "hiveserde"],
    workflow: str,
    make_table_migration_context,
) -> None:
    """The migration status should be refreshed after the migration job."""
    tables, _ = make_table_migration_context(scenario, installation_ctx)
    ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            skip_tacl_migration=True,
        ),
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


def test_table_migration_convert_manged_to_external(installation_ctx, make_table_migration_context) -> None:
    tables, dst_schema = make_table_migration_context("managed", installation_ctx)
    ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            skip_tacl_migration=True,
        ),
        extend_prompts={
            r"If hive_metastore contains managed table with external.*": "0",
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )

    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow("migrate-tables")

    for table in tables.values():
        try:
            assert ctx.workspace_client.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"
    managed_table = tables["src_managed_table"]

    for key, value, _ in ctx.sql_backend.fetch(
        f"DESCRIBE TABLE EXTENDED {escape_sql_identifier(managed_table.full_name)}"
    ):
        if key == "Type":
            assert value == "EXTERNAL"
            break


@pytest.mark.parametrize("workflow", ["migrate-external-hiveserde-tables-in-place-experimental", "migrate-external-tables-ctas"])
def test_hiveserde_table_in_place_migration_job(installation_ctx, make_table_migration_context, workflow) -> None:
    tables, dst_schema = make_table_migration_context("hiveserde", installation_ctx)
    ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            skip_tacl_migration=True,
        ),
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()

    ctx.deployed_workflows.run_workflow(workflow, skip_job_wait=True)

    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"
    for table in tables.values():
        try:
            assert ctx.workspace_client.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"


def test_table_migration_job_publishes_remaining_tables(installation_ctx, make_table_migration_context) -> None:
    tables, dst_schema = make_table_migration_context("regular", installation_ctx)
    ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            skip_tacl_migration=True,
        ),
    )
    ctx.workspace_installation.run()
    second_table = list(tables.values())[1]
    table = Table(
        "hive_metastore",
        dst_schema.name,
        second_table.name,
        object_type="UNKNOWN",
        table_format="UNKNOWN",
    )
    ctx.table_mapping.skip_table_or_view(dst_schema.name, second_table.name, load_table=lambda *_: table)

    ctx.deployed_workflows.run_workflow("migrate-tables")

    assert ctx.deployed_workflows.validate_step("migrate-tables")
    remaining_tables = list(
        ctx.sql_backend.fetch(
            f"""
                SELECT
                SUBSTRING(message, LENGTH('remained-hive-metastore-table: ') + 1)
                AS message
                FROM {ctx.inventory_database}.logs
                WHERE message LIKE 'remained-hive-metastore-table: %'
            """
        )
    )
    assert remaining_tables[0].message == f'hive_metastore.{dst_schema.name}.{second_table.name}'
