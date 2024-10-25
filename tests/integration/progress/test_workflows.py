import datetime as dt

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried

from ..contexts.installation import MockInstallationContext


@retried(on=[NotFound, InvalidParameterValue], timeout=dt.timedelta(minutes=12))
def test_running_real_migration_progress_job(installation_ctx: MockInstallationContext) -> None:
    """Ensure that the migration-progress workflow can complete successfully."""
    # Limit the resources crawled by the assessment
    source_schema = installation_ctx.make_schema()
    installation_ctx.make_table(schema_name=source_schema.name)
    installation_ctx.make_linting_resources()
    installation_ctx.make_group()

    installation_ctx.workspace_installation.run()

    # The assessment workflow is a prerequisite for migration-progress: it needs to successfully complete before we can
    # test the migration-progress workflow.
    workflow = "assessment"
    installation_ctx.deployed_workflows.run_workflow(workflow)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # After the assessment, a user (maybe) installs the progress tracking
    installation_ctx.progress_tracking_installation.run()

    # Run the migration-progress workflow until completion.
    workflow = "migration-progress-experimental"
    installation_ctx.deployed_workflows.run_workflow(workflow)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # Ensure that the migration-progress workflow populated the `workflow_runs` table.
    query = f"SELECT 1 FROM {installation_ctx.ucx_catalog}.multiworkspace.workflow_runs LIMIT 1"
    assert any(installation_ctx.sql_backend.fetch(query)), f"No workflow run captured: {query}"

    # Ensure that the history file has records written to it.
    query = f"SELECT 1 from {installation_ctx.ucx_catalog}.multiworkspace.historical LIMIT 1"
    assert any(installation_ctx.sql_backend.fetch(query)), f"No snapshots captured to the history log: {query}"
