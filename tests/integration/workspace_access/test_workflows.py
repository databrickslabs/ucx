from dataclasses import replace


from databricks.sdk.service import sql
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.workspace import AclPermission


def test_running_real_migrate_groups_job(
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_secret_scope,
    make_secret_scope_acl,
):
    ws_group_a, acc_group_a = installation_ctx.make_ucx_group(wait_for_provisioning=True)

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

    installation_ctx.deployed_workflows.run_workflow("migrate-groups")

    # specific permissions api migrations are checked in different and smaller integration tests
    found = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert acc_group_a.display_name in found, "Group not found in cluster policies"
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE

    scope_permission = installation_ctx.secret_scope_acl_support.secret_scope_permission(
        secret_scope, acc_group_a.display_name
    )
    assert scope_permission == AclPermission.WRITE


def test_running_legacy_validate_groups_permissions_job(
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
    installation_ctx.__dict__['config_transform'] = lambda c: replace(c, use_legacy_permission_migration=True)
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.snapshot()

    # assert the job does not throw any exception
    installation_ctx.deployed_workflows.run_workflow("validate-groups-permissions")
