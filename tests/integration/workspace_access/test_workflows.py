from datetime import timedelta

import pytest
from databricks.labs.blueprint.parallel import ManyError

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service import sql
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.workspace import AclPermission


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=8))
def test_running_real_migrate_groups_job(
    ws,
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_secret_scope,
    make_secret_scope_acl,
):
    ws_group_a, acc_group_a = installation_ctx.make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    table = installation_ctx.make_table()
    installation_ctx.make_grant(ws_group_a.display_name, "SELECT", table_info=table)

    secret_scope = make_secret_scope()
    make_secret_scope_acl(scope=secret_scope, principal=ws_group_a.display_name, permission=AclPermission.WRITE)

    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.__dict__['include_object_permissions'] = [
        f"cluster-policies:{cluster_policy.policy_id}",
        f"TABLE:{table.full_name}",
        f"secrets:{secret_scope}",
    ]

    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.snapshot()

    installation_ctx.deployed_workflows.run_workflow("migrate-groups")

    found = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE
    assert found[f"{installation_ctx.config.renamed_group_prefix}{ws_group_a.display_name}"] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_validate_groups_permissions_job(
    installation_ctx,
    make_query,
    make_query_permissions,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_secret_scope,
    make_secret_scope_acl,
):
    ws_group_a, _ = installation_ctx.make_ucx_group()

    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group_a.display_name,
    )

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    table = installation_ctx.make_table()
    installation_ctx.make_grant(ws_group_a.display_name, "SELECT", table_info=table)

    secret_scope = make_secret_scope()
    make_secret_scope_acl(scope=secret_scope, principal=ws_group_a.display_name, permission=AclPermission.WRITE)

    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.__dict__['include_object_permissions'] = [
        f"cluster-policies:{cluster_policy.policy_id}",
        f"queries:{query.id}",
        f"TABLE:{table.full_name}",
        f"secrets:{secret_scope}",
    ]
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.snapshot()

    # assert the job does not throw any exception
    installation_ctx.deployed_workflows.run_workflow("validate-groups-permissions")


@retried(on=[NotFound], timeout=timedelta(minutes=8))
def test_running_real_validate_groups_permissions_job_fails(
    ws, installation_ctx, make_cluster_policy, make_cluster_policy_permissions
):

    ws_group_a, _ = installation_ctx.make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    installation_ctx.make_schema()  # optimization to skip listing all schemas
    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.__dict__['include_object_permissions'] = [f'cluster-policies:{cluster_policy.policy_id}']
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.snapshot()

    # remove permission so the validation fails
    ws.permissions.set(
        request_object_type="cluster-policies", request_object_id=cluster_policy.policy_id, access_control_list=[]
    )

    with pytest.raises(ManyError):
        installation_ctx.deployed_workflows.run_workflow("validate-groups-permissions")
