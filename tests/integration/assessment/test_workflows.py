from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.jobs import RunLifecycleStateV2State, TerminationCodeCode


def _assert_run_success(ws: WorkspaceClient, run_id: int) -> None:
    """Verify that a job run completed successfully."""
    run = ws.jobs.get_run(run_id=run_id)
    assert run.status.state == RunLifecycleStateV2State.TERMINATED
    assert run.status.termination_details.code == TerminationCodeCode.SUCCESS


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=8))
def test_running_real_assessment_job(ws, installation_ctx, make_cluster_policy, make_cluster_policy_permissions):
    ws_group_a, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()

    run_id = installation_ctx.deployed_workflows.run_workflow("assessment")
    _assert_run_success(ws, run_id)

    after = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[ws_group_a.display_name] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=12))
def test_running_real_migration_progress_job(
    ws,
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
) -> None:
    """Ensure that the migration-progress workflow can complete successfully."""
    ws_group_a, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()

    # The assessment workflow needs to successfully complete before we can test migration progress.
    run_id = installation_ctx.deployed_workflows.run_workflow("assessment")
    _assert_run_success(ws, run_id)

    # Run the migration-progress workflow until completion.
    run_id = installation_ctx.deployed_workflows.run_workflow("migration-progress-experimental")
    _assert_run_success(ws, run_id)
