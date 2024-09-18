from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.jobs import RunLifecycleStateV2State, TerminationCodeCode

from ..conftest import MockInstallationContext

def _assert_run_success(ws: WorkspaceClient, run_id: int) -> None:
    """Verify that a job run completed successfully."""
    run = ws.jobs.get_run(run_id=run_id)
    assert run.status and run.status.state == RunLifecycleStateV2State.TERMINATED
    assert run.status.termination_details and run.status.termination_details.code == TerminationCodeCode.SUCCESS


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=12))
def test_running_real_migration_progress_job(ws: WorkspaceClient, installation_ctx: MockInstallationContext) -> None:
    """Ensure that the migration-progress workflow can complete successfully."""
    installation_ctx.workspace_installation.run()

    # The assessment workflow is a prerequisite for migration-progress: it needs to successfully complete before we can
    # test the migration-progress workflow.
    run_id = installation_ctx.deployed_workflows.run_workflow("assessment")
    _assert_run_success(ws, run_id)

    # Run the migration-progress workflow until completion.
    run_id = installation_ctx.deployed_workflows.run_workflow("migration-progress-experimental")
    _assert_run_success(ws, run_id)
