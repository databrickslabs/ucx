from datetime import timedelta

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried

from ..conftest import MockInstallationContext



@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=12))
def test_running_real_migration_progress_job(installation_ctx: MockInstallationContext) -> None:
    """Ensure that the migration-progress workflow can complete successfully."""
    installation_ctx.workspace_installation.run()

    # The assessment workflow is a prerequisite for migration-progress: it needs to successfully complete before we can
    # test the migration-progress workflow.
    installation_ctx.deployed_workflows.run_workflow("assessment")
    assert installation_ctx.deployed_workflows.validate_step("assessment")

    # Run the migration-progress workflow until completion.
    installation_ctx.deployed_workflows.run_workflow("migration-progress-experimental")
    assert installation_ctx.deployed_workflows.validate_step("migration-progress-experimental")
