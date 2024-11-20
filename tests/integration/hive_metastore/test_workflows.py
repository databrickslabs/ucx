import pytest
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation


@pytest.mark.parametrize(
    "prepare_tables_for_migration,workflow",
    [
        ("regular", "migrate-tables"),
        ("hiveserde", "migrate-external-hiveserde-tables-in-place-experimental"),
        ("hiveserde", "migrate-external-tables-ctas"),
        # TODO: Some workflows are missing here, and also need to be included in the tests.
    ],
    indirect=("prepare_tables_for_migration",),
)
def test_table_migration_job_refreshes_migration_status(
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
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test these workflows.
    installation_ctx.deployed_workflows.run_workflow("assessment")
    assert installation_ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    run_id = ctx.deployed_workflows.run_workflow(workflow)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

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
        SELECT 1 FROM {installation_ctx.ucx_catalog}.multiworkspace.workflow_runs
        WHERE workspace_id = {installation_ctx.workspace_id}
          AND workflow_run_id = {run_id}
        LIMIT 1
    """
    assert any(installation_ctx.sql_backend.fetch(query)), f"No workflow run captured: {query}"

    # Ensure that the history file has table records written to it that correspond to this run.
    query = f"""
        SELECT 1 from {installation_ctx.ucx_catalog}.multiworkspace.historical
        WHERE workspace_id = {installation_ctx.workspace_id}
          AND job_run_id = {run_id}
          AND object_type = 'Table'
        LIMIT 1
    """
    assert any(installation_ctx.sql_backend.fetch(query)), f"No snapshots captured to the history log: {query}"


@pytest.mark.parametrize(
    "prepare_tables_for_migration,workflow",
    [
        ("managed", "migrate-tables"),
    ],
    indirect=("prepare_tables_for_migration",),
)
def test_table_migration_for_managed_table(ws, installation_ctx, prepare_tables_for_migration, workflow, sql_backend):
    # This test cases test the CONVERT_TO_EXTERNAL scenario.
    tables, dst_schema = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r"If hive_metastore contains managed table with external.*": "0",
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    installation_ctx.deployed_workflows.run_workflow("assessment")
    assert installation_ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow(workflow)

    for table in tables.values():
        try:
            assert ws.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"
    managed_table = tables["src_managed_table"]

    for key, value, _ in sql_backend.fetch(f"DESCRIBE TABLE EXTENDED {escape_sql_identifier(managed_table.full_name)}"):
        if key == "Type":
            assert value == "EXTERNAL"
            break


@pytest.mark.parametrize('prepare_tables_for_migration', [('hiveserde')], indirect=True)
def test_hiveserde_table_in_place_migration_job(ws, installation_ctx, prepare_tables_for_migration):
    tables, dst_schema = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    installation_ctx.deployed_workflows.run_workflow("assessment")
    assert installation_ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow("migrate-external-hiveserde-tables-in-place-experimental")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-external-hiveserde-tables-in-place-experimental")
    # assert the tables are migrated
    for table in tables.values():
        try:
            assert ws.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"


@pytest.mark.parametrize('prepare_tables_for_migration', [('hiveserde')], indirect=True)
def test_hiveserde_table_ctas_migration_job(ws, installation_ctx, prepare_tables_for_migration):
    tables, dst_schema = prepare_tables_for_migration
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()
    ProgressTrackingInstallation(ctx.sql_backend, ctx.ucx_catalog).run()

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    installation_ctx.deployed_workflows.run_workflow("assessment")
    assert installation_ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    ctx.deployed_workflows.run_workflow("migrate-external-tables-ctas")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-external-tables-ctas")
    # assert the tables are migrated
    for table in tables.values():
        try:
            assert ws.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"


@pytest.mark.parametrize('prepare_tables_for_migration', ['regular'], indirect=True)
def test_table_migration_job_publishes_remaining_tables(
    ws, installation_ctx, sql_backend, prepare_tables_for_migration, caplog
):
    tables, dst_schema = prepare_tables_for_migration
    installation_ctx.workspace_installation.run()
    ProgressTrackingInstallation(installation_ctx.sql_backend, installation_ctx.ucx_catalog).run()
    second_table = list(tables.values())[1]
    table = Table(
        "hive_metastore",
        dst_schema.name,
        second_table.name,
        object_type="UNKNOWN",
        table_format="UNKNOWN",
    )
    installation_ctx.table_mapping.skip_table_or_view(dst_schema.name, second_table.name, load_table=lambda *_: table)

    # The assessment workflow is a prerequisite, and now verified by the workflow: it needs to successfully complete
    # before we can test the migration workflow.
    installation_ctx.deployed_workflows.run_workflow("assessment")
    assert installation_ctx.deployed_workflows.validate_step("assessment"), "Workflow failed: assessment"

    # The workflow under test.
    installation_ctx.deployed_workflows.run_workflow("migrate-tables")
    assert installation_ctx.deployed_workflows.validate_step("migrate-tables")

    remaining_tables = list(
        sql_backend.fetch(
            f"""
                SELECT
                SUBSTRING(message, LENGTH('remained-hive-metastore-table: ') + 1)
                AS message
                FROM {installation_ctx.inventory_database}.logs
                WHERE message LIKE 'remained-hive-metastore-table: %'
            """
        )
    )
    assert remaining_tables[0].message == f'hive_metastore.{dst_schema.name}.{second_table.name}'
