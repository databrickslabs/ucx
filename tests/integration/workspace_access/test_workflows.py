import datetime as dt
from dataclasses import replace

import pytest
from databricks.sdk.retries import retried
from databricks.sdk.service import sql
from databricks.sdk.service.iam import Group, PermissionLevel
from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.workspace_access.groups import MigratedGroup


def test_running_real_migrate_groups_job(
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_secret_scope,
    make_secret_scope_acl,
):
    installation_ctx = installation_ctx.replace(
        config_transform=lambda wc: replace(
            wc,
            use_legacy_permission_migration=True,
        ),
    )
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

    installation_ctx.deployed_workflows.run_workflow("migrate-groups-legacy")

    @retried(on=[KeyError], timeout=dt.timedelta(minutes=1))
    def get_workspace_group(display_name: str) -> Group:
        for grp in installation_ctx.workspace_client.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    @retried(on=[KeyError], timeout=dt.timedelta(minutes=1))
    def get_account_group(display_name: str) -> Group:
        for grp in installation_ctx.account_client.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    # The account group should exist, not the original workspace group
    renamed_workspace_group_name = installation_ctx.renamed_group_prefix + ws_group_a.display_name
    assert get_workspace_group(renamed_workspace_group_name), f"Renamed workspace group not found: {renamed_workspace_group_name}"
    assert get_account_group(acc_group_a.display_name), f"Account group not found: {acc_group_a.display_name}"

    # specific permissions api migrations are checked in different and smaller integration tests
    found = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert acc_group_a.display_name in found, "Group not found in cluster policies"
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE

    scope_permission = installation_ctx.secret_scope_acl_support.secret_scope_permission(
        secret_scope, acc_group_a.display_name
    )
    assert scope_permission == AclPermission.WRITE

    # The original workspace group should not exist, testing as last due to wait on timeout
    with pytest.raises(TimeoutError):
        get_workspace_group(ws_group_a.display_name)


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


def test_permissions_migration_for_group_with_same_name(
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
):
    ws_group, acc_group = installation_ctx.make_ucx_group()
    migrated_group = MigratedGroup.partial_info(ws_group, acc_group)
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=migrated_group.name_in_workspace,
    )

    schema_a = installation_ctx.make_schema()
    table_a = installation_ctx.make_table(schema_name=schema_a.name)
    installation_ctx.make_grant(migrated_group.name_in_workspace, 'USAGE', schema_info=schema_a)
    installation_ctx.make_grant(migrated_group.name_in_workspace, 'OWN', schema_info=schema_a)
    installation_ctx.make_grant(migrated_group.name_in_workspace, 'SELECT', table_info=table_a)

    installation_ctx.workspace_installation.run()

    installation_ctx.deployed_workflows.run_workflow("migrate-groups")

    object_permissions = installation_ctx.generic_permissions_support.load_as_dict(
        "cluster-policies", cluster_policy.policy_id
    )
    new_schema_grants = installation_ctx.grants_crawler.for_schema_info(schema_a)

    if {"USAGE", "OWN"} != new_schema_grants[migrated_group.name_in_account] or object_permissions[
        migrated_group.name_in_account
    ] != PermissionLevel.CAN_USE:
        installation_ctx.deployed_workflows.relay_logs("migrate-groups")
    assert {"USAGE", "OWN"} == new_schema_grants[
        migrated_group.name_in_account
    ], "Incorrect schema grants for migrated group"
    assert (
        object_permissions[migrated_group.name_in_account] == PermissionLevel.CAN_USE
    ), "Incorrect permissions for migrated group"
