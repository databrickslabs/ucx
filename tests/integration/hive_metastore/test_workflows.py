import dataclasses
from typing import Literal

import pytest
from databricks.labs.lsql.core import Row

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation


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
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test these workflows.
    ctx.deployed_workflows.run_workflow("assessment", skip_job_wait=True)
    assessment_completed_correctly = ctx.deployed_workflows.validate_step("assessment")
    assert assessment_completed_correctly, "Workflow failed: assessment"

    # The workflow under test.
    run_id = ctx.deployed_workflows.run_workflow(workflow, skip_job_wait=True)
    workflow_completed_correctly = ctx.deployed_workflows.validate_step(workflow)
    assert workflow_completed_correctly, f"Workflow failed: {workflow}"

    # Avoiding MigrationStatusRefresh as it will refresh the status before fetching.
    migration_status_query = f"SELECT * FROM {ctx.migration_status_refresher.full_name}"
    migration_statuses = list(ctx.sql_backend.fetch(migration_status_query))

    if not migration_statuses:
        ctx.deployed_workflows.relay_logs(workflow)
        pytest.fail("No migration statuses found")

    problems = []
    for table in tables.values():
        migration_status = []
        for status in migration_statuses:
            if status.src_schema == table.schema_name and status.src_table == table.name:
                migration_status.append(status)

        match migration_status:
            case []:
                problems.append(f"No migration status found for {table.table_type} {table.full_name}")
            case [_, _, *_]:
                problems.append(f"Multiple migration statuses found for {table.table_type} {table.full_name}")
            case [status] if status.dst_schema is None:
                problems.append(f"No destination schema found for {table.table_type} {table.full_name}")
            case [status] if status.dst_table is None:
                problems.append(f"No destination table found for {table.table_type} {table.full_name}")

    failure_message = (
        "\n".join(problems) + " given migration statuses:\n" + "\n".join([str(status) for status in migration_statuses])
    )
    assert not problems, failure_message

    # Ensure that the workflow populated the `workflow_runs` table.
    query = f"""
        SELECT 1 FROM {ctx.ucx_catalog}.multiworkspace.workflow_runs
        WHERE workspace_id = {ctx.workspace_id}
          AND workflow_run_id = {run_id}
        LIMIT 1
    """
    assert any(ctx.sql_backend.fetch(query)), f"No workflow run captured: {query}"

    # Ensure that the history file has table records written to it that correspond to this run.
    query = f"""
        SELECT 1 from {ctx.ucx_catalog}.multiworkspace.historical
        WHERE workspace_id = {ctx.workspace_id}
          AND job_run_id = {run_id}
          AND object_type = 'Table'
        LIMIT 1
    """
    assert any(ctx.sql_backend.fetch(query)), f"No snapshots captured to the history log: {query}"


def test_table_migration_convert_manged_to_external(installation_ctx, make_table_migration_context) -> None:
    """Convert managed tables to external before migrating.

    Note:
        This test fails from Databricks runtime 16.0 (https://docs.databricks.com/en/release-notes/runtime/16.0.html),
        probably due to the JDK update (https://docs.databricks.com/en/release-notes/runtime/16.0.html#breaking-change-jdk-17-is-now-the-default).
    """
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
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    ctx.deployed_workflows.run_workflow("assessment")
    assert ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow("migrate-tables")
    assert ctx.deployed_workflows.validate_step("migrate-tables")

    missing_tables = set[str]()
    for table in tables.values():
        migrated_table_name = f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}"
        if not ctx.workspace_client.tables.exists(migrated_table_name):
            missing_tables.add(migrated_table_name)
    assert not missing_tables, f"Missing migrated tables: {missing_tables}"

    managed_table = tables["src_managed_table"]
    for key, value, _ in ctx.sql_backend.fetch(
        f"DESCRIBE TABLE EXTENDED {escape_sql_identifier(managed_table.full_name)}"
    ):
        if key == "Type":
            assert value == "EXTERNAL"
            break


@pytest.mark.parametrize(
    "workflow", ["migrate-external-hiveserde-tables-in-place-experimental", "migrate-external-tables-ctas"]
)
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
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    ctx.deployed_workflows.run_workflow("assessment")
    assert ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow(workflow, skip_job_wait=True)
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"
    # assert the tables are migrated
    missing_tables = set[str]()
    for table in tables.values():
        migrated_table_name = f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}"
        if not ctx.workspace_client.tables.exists(migrated_table_name):
            missing_tables.add(migrated_table_name)
    assert not missing_tables, f"Missing migrated tables: {missing_tables}"


def test_hiveserde_table_ctas_migration_job(ws, installation_ctx, make_table_migration_context) -> None:
    tables, dst_schema = make_table_migration_context("hiveserde", installation_ctx)
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    ctx.deployed_workflows.run_workflow("assessment")
    assert ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow("migrate-external-tables-ctas")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-external-tables-ctas")
    # assert the tables are migrated
    missing_tables = set[str]()
    for table in tables.values():
        migrated_table_name = f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}"
        if not ctx.workspace_client.tables.exists(migrated_table_name):
            missing_tables.add(migrated_table_name)
    assert not missing_tables, f"Missing migrated tables: {missing_tables}"


def test_table_migration_job_publishes_remaining_tables(installation_ctx, make_table_migration_context) -> None:
    tables, dst_schema = make_table_migration_context("regular", installation_ctx)
    ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            skip_tacl_migration=True,
        ),
    )
    ctx.workspace_installation.run()
    ProgressTrackingInstallation(installation_ctx.sql_backend, installation_ctx.ucx_catalog).run()
    second_table = list(tables.values())[1]
    table = Table(
        "hive_metastore",
        dst_schema.name,
        second_table.name,
        object_type="UNKNOWN",
        table_format="UNKNOWN",
    )
    ctx.table_mapping.skip_table_or_view(dst_schema.name, second_table.name, load_table=lambda *_: table)

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    ctx.deployed_workflows.run_workflow("assessment", skip_job_wait=True)
    assert ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow("migrate-tables", skip_job_wait=True)
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
    assert remaining_tables == [Row(message=f"hive_metastore.{dst_schema.name}.{second_table.name}")]
