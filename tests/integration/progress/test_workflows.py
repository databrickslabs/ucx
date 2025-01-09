import datetime as dt

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.jobs import PauseStatus

from ..conftest import MockInstallationContext


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
    installation_ctx.deployed_workflows.run_workflow(workflow, skip_job_wait=True)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # After the assessment, a user (maybe) installs the progress tracking
    installation_ctx.progress_tracking_installation.run()

    # Run the migration-progress workflow until completion.
    workflow = "migration-progress-experimental"
    installation_ctx.deployed_workflows.run_workflow(workflow, skip_job_wait=True)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # Ensure that the `migration-progress` workflow populated the `workflow_runs` table.
    query = f"SELECT 1 FROM {installation_ctx.ucx_catalog}.multiworkspace.workflow_runs LIMIT 1"
    assert any(installation_ctx.sql_backend.fetch(query)), f"No workflow run captured: {query}"

    # Ensure that the history file has records written to it.
    query = f"SELECT 1 from {installation_ctx.ucx_catalog}.multiworkspace.historical LIMIT 1"
    assert any(installation_ctx.sql_backend.fetch(query)), f"No snapshots captured to the history log: {query}"


def test_migration_progress_job_has_schedule(ws: WorkspaceClient, installation_ctx: MockInstallationContext) -> None:
    """Ensure that the migration-progress workflow is installed with a schedule attached."""
    installation_ctx.workspace_installation.run()

    workflow_id = installation_ctx.install_state.jobs["migration-progress-experimental"]
    workflow = ws.jobs.get(workflow_id)
    assert workflow.settings
    schedule = workflow.settings.schedule
    assert schedule is not None, "No schedule found for the migration-progress workflow."
    assert schedule.quartz_cron_expression, "No cron expression found for the migration-progress workflow."
    assert schedule.timezone_id, "No time-zone specified for scheduling the migration-progress workflow."
    assert schedule.pause_status == PauseStatus.UNPAUSED, "Workflow schedule should not be paused."
