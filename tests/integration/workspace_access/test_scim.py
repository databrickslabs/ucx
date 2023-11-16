from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_scim(ws: WorkspaceClient, make_ucx_group, sql_backend, inventory_schema, make_pipeline):
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
    make_pipeline()

    # Task 1 - crawl_permissions
    scim_support = ScimSupport(ws)
    pi = PermissionManager(sql_backend, inventory_schema, [scim_support])
    pi.cleanup()
    pi.inventorize_permissions()

    # Task 2 - crawl_groups
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=[ws_group.display_name])
    group_manager.snapshot()

    # Task 2 - apply_permissions_to_backup_groups
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=[ws_group.display_name])
    group_manager.rename_groups()

    # Task 3 - reflect_account_groups_on_workspace
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=[ws_group.display_name])
    group_manager.reflect_account_groups_on_workspace()

    # Task 4 - apply_permissions_to_account_groups
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=[ws_group.display_name])
    migration_state = group_manager.get_migration_state()
    permission_manager = PermissionManager.factory(ws, sql_backend, inventory_database=inventory_schema)
    permission_manager.apply_group_permissions(migration_state)

    old_group = ws.groups.get(ws_group.id)
    reflected_group = ws.groups.get(acc_group.id)

    assert reflected_group.display_name == ws_group.display_name
    assert reflected_group.id == acc_group.id
    assert len(reflected_group.members) == len(old_group.members)
    assert len(reflected_group.entitlements) == len(old_group.entitlements)
