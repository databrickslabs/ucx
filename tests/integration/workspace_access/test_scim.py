from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.groups import (
    GroupManager,
)
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_scim(ws: WorkspaceClient, make_ucx_group, sql_backend, inventory_schema):
    """
    This test does the following:
    * create a ws group with roles and entitlements
    * migrate this group
    * verify that the migrated group has the same roles and entitlements
    :return:
    """
    ws_group, acc_group = make_ucx_group()
    ws.groups.patch(
        ws_group.id,
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path="entitlements",
                value=[iam.ComplexValue(value="databricks-sql-access").as_dict()],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )
    groups_config = GroupsConfig(selected=[ws_group.display_name])

    # Task 1 - crawl_permissions
    scim_support = ScimSupport(ws)
    pi = PermissionManager(sql_backend, inventory_schema, [scim_support])
    pi.cleanup()
    pi.inventorize_permissions()

    # Task 2 - apply_permissions_to_backup_groups
    group_manager = GroupManager(ws, groups_config)
    group_manager.prepare_groups_in_environment()
    pi.apply_group_permissions(group_manager.migration_state, destination="backup")
    group_manager.migration_state.persist_migration_state(sql_backend, inventory_schema)

    # Task 3 - replace the groups in the workspace with the account groups
    group_manager = GroupManager(ws, groups_config)
    remote_state = GroupMigrationState().fetch_migration_state(sql_backend, inventory_schema)
    group_manager.replace_workspace_groups_with_account_groups(remote_state)

    # Task 4 - apply_permissions_to_account_groups
    remote_state = group_manager.migration_state.fetch_migration_state(sql_backend, inventory_schema)
    migration_state = GroupManager.prepare_apply_permissions_to_account_groups(
        ws, remote_state, groups_config.backup_group_prefix
    )
    pi.apply_group_permissions(migration_state, destination="account")

    assert len(ws.groups.get(acc_group.id).members) == len(ws_group.members)
