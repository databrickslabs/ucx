import dataclasses

from databricks.labs.lsql.backends import CommandExecutionBackend
from databricks.sdk.service.iam import PermissionLevel


def test_running_real_assessment_job_ext_hms(
    ws,
    installation_ctx,
    env_or_skip,
    make_cluster_policy,
    make_cluster_policy_permissions,
) -> None:
    cluster_id = env_or_skip('TEST_EXT_HMS_CLUSTER_ID')
    ext_hms_ctx = installation_ctx.replace(
        sql_backend=CommandExecutionBackend(ws, cluster_id),
        config_transform=lambda wc: dataclasses.replace(
            wc,
            override_clusters=None,
            include_object_permissions=[f"cluster-policies:{cluster_policy.policy_id}"],
        ),
        extend_prompts={
            r"Instance pool id to be set.*": env_or_skip("TEST_INSTANCE_POOL_ID"),
            r".*Do you want to update the existing installation?.*": 'yes',
            r".*connect to the external metastore?.*": "yes",
            r"Choose a cluster policy": "0",
        },
    )
    ws_group, _ = ext_hms_ctx.make_ucx_group(wait_for_provisioning=True)
    # TODO: Move `make_cluster_policy` and `make_cluster_policy_permissions` to context like other `make_` methods
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    ext_hms_ctx.make_linting_resources()
    source_schema = ext_hms_ctx.make_schema(catalog_name="hive_metastore")
    ext_hms_ctx.make_table(schema_name=source_schema.name)
    ext_hms_ctx.workspace_installation.run()

    workflow = "assessment"
    ext_hms_ctx.deployed_workflows.run_workflow(workflow)
    assert ext_hms_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    after = ext_hms_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert ws_group.display_name in after, f"Group {ws_group.display_name} not found in cluster policy"
    assert after[ws_group.display_name] == PermissionLevel.CAN_USE
