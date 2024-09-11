from datetime import timedelta

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel


@retried(on=[NotFound, InvalidParameterValue])
def test_running_real_assessment_job(
    ws,
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
    populate_for_linting,
):
    ws_group, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()

    populate_for_linting(installation_ctx.installation)

    installation_ctx.deployed_workflows.run_workflow("assessment", max_wait=timedelta(minutes=25))
    assert installation_ctx.deployed_workflows.validate_step("assessment")

    after = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)

    assert after[ws_group.display_name] == PermissionLevel.CAN_USE

