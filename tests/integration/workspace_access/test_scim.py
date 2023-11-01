from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.scim import ScimSupport


def _patch_by_id(ws: WorkspaceClient, group_id: str, path: Literal["roles", "entitlements"],
                 value: list[iam.ComplexValue]):
    ws.groups.patch(
        group_id,
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path=path,
                value=[_.as_dict() for _ in value],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_scim(ws: WorkspaceClient, make_ucx_group, sql_backend, inventory_schema):
    """
    This test does the following:
    * create a ws group with roles and entitlements
    * migrate this group
    * verify that the migrated group has the same roles and entitlements
    :return:
    """
    ws_group, acc_group = make_ucx_group()

    _patch_by_id(ws, ws_group.id, "entitlements", [iam.ComplexValue(value="databricks-sql-access")])

    group_manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    group_manager.prepare_groups_in_environment()

    scim_support = ScimSupport(ws)
    pi = PermissionManager(sql_backend, inventory_schema, [scim_support])
    pi.cleanup()
    pi.inventorize_permissions()
    pi.apply_group_permissions(group_manager.migration_state, destination="backup")
    group_manager.replace_workspace_groups_with_account_groups()
    pi.apply_group_permissions(group_manager.migration_state, destination="account")
    assert iam.ComplexValue(value="databricks-sql-access") in ws.groups.get(acc_group.id).entitlements
