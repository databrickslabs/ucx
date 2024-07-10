from datetime import timedelta

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=8))
def test_running_real_assessment_job(ws, installation_ctx, make_cluster_policy, make_cluster_policy_permissions):

    ctx = installation_ctx.replace(skip_dashboards=False)
    ws_group_a, _ = ctx.make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    ctx.workspace_installation.run()

    ctx.deployed_workflows.run_workflow("assessment")

    after = ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[ws_group_a.display_name] == PermissionLevel.CAN_USE
