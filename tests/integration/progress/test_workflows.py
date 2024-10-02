import datetime as dt

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried

from ..conftest import MockInstallationContext


@retried(on=[NotFound, InvalidParameterValue], timeout=dt.timedelta(minutes=12))
def test_running_real_migration_progress_job(installation_ctx: MockInstallationContext) -> None:
    """Ensure that the migration-progress workflow can complete successfully."""
    installation_ctx.workspace_installation.run()

    # The assessment workflow is a prerequisite for migration-progress: it needs to successfully complete before we can
    # test the migration-progress workflow.
    workflow = "assessment"
    installation_ctx.deployed_workflows.run_workflow(workflow, max_wait=dt.timedelta(minutes=25))
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # After the assessment, a user (maybe) installs the progress tracking
    installation_ctx.progress_tracking_installation.run()

    # Run the migration-progress workflow until completion.
    workflow = "migration-progress-experimental"
    installation_ctx.deployed_workflows.run_workflow(workflow)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # Ensure that the migration-progress workflow populated the `workflow_runs` table.
    query = f"SELECT 1 FROM {installation_ctx.ucx_catalog}.multiworkspace.workflow_runs"
    assert any(installation_ctx.sql_backend.fetch(query)), f"No workflow run captured: {query}"
