import datetime as dt
from dataclasses import replace

import pytest
from databricks.sdk.retries import retried
from databricks.sdk.service import sql
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.workspace import AclPermission


def test_running_real_migrate_groups_job(
    installation_ctx,
) -> None:
    """Test the migrate groups workflow.

    We have many asserts in a single integration tests as we minimize the number of integration tests that run workflows
    to minimize the number of long-running integration tests.

    For testing, we require:
    - UCX installation (as always)
    - A workspace group as the source for migration
    - A account group as the target for migration
    - Permissions to migrate:
      - Cluster policy
      - Schema and table access permissions
      - Secret scope permissions
    - Inventory tables used by the migrate groups workflow

    We test:
    - The workflow to complete successfully
    - The workspace group to be renamed
    - The permissions to be transferred to the account group
    """
    ws_group, acc_group = installation_ctx.make_ucx_group(wait_for_provisioning=True)

    cluster_policy = installation_ctx.make_cluster_policy()
    installation_ctx.make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )

    schema = installation_ctx.make_schema()
    table = installation_ctx.make_table(schema_name=schema.name)
    installation_ctx.make_grant(ws_group.display_name, 'USAGE', schema_info=schema)
    installation_ctx.make_grant(ws_group.display_name, 'OWN', schema_info=schema)
    installation_ctx.make_grant(ws_group.display_name, 'SELECT', table_info=table)

    secret_scope = installation_ctx.make_secret_scope()
    installation_ctx.make_secret_scope_acl(
        scope=secret_scope, principal=ws_group.display_name, permission=AclPermission.WRITE
    )

    installation_ctx.workspace_installation.run()
    # The crawlers should run as part of the assessment. To minimize the crawling here, we only crawl what is necessary
    # Tables crawler fails on `tacl` cluster used by the apply and validate permission tasks
    installation_ctx.tables_crawler.snapshot(force_refresh=True)

    workflow = "migrate-groups"
    installation_ctx.deployed_workflows.run_workflow(workflow, skip_job_wait=True)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    # Wrapper functions to wait for eventual consistency of API
    @retried(on=[KeyError], timeout=dt.timedelta(minutes=1))
    def wait_for_workspace_group_to_exists(display_name: str) -> bool:
        if installation_ctx.group_manager.has_workspace_group(display_name):
            return True
        raise KeyError(f"Group not found {display_name}")

    # The original workspace group should be renamed
    renamed_workspace_group_name = installation_ctx.renamed_group_prefix + ws_group.display_name
    assert wait_for_workspace_group_to_exists(
        renamed_workspace_group_name
    ), f"Workspace group not found: {renamed_workspace_group_name}"
    if installation_ctx.group_manager.has_workspace_group(ws_group.display_name):  # Avoid wait on timeout
        with pytest.raises(TimeoutError):
            wait_for_workspace_group_to_exists(ws_group.display_name)  # Expect to NOT exists

    schema_grants = installation_ctx.grants_crawler.for_schema_info(schema)
    assert {"USAGE", "OWN"} == schema_grants[acc_group.display_name], "Incorrect schema grants for migrated group"

    # specific permissions api migrations are checked in different and smaller integration tests
    object_permissions = installation_ctx.generic_permissions_support.load_as_dict(
        "cluster-policies", cluster_policy.policy_id
    )
    assert acc_group.display_name in object_permissions, "Group not found in cluster policies"
    assert object_permissions[acc_group.display_name] == PermissionLevel.CAN_USE

    scope_permission = installation_ctx.secret_scope_acl_support.secret_scope_permission(
        secret_scope, acc_group.display_name
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
    installation_ctx.__dict__['config_transform'] = lambda c: replace(c, use_legacy_permission_migration=True)
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.snapshot()

    # assert the job does not throw any exception
    installation_ctx.deployed_workflows.run_workflow("validate-groups-permissions")
