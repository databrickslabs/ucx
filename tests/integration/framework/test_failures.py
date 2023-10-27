from databricks.labs.ucx.workspace_access.groups import GroupMigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_failures(sql_backend, inventory_schema, ws, make_group):
    group = make_group()
    pm = PermissionManager(sql_backend, inventory_schema, [ScimSupport(ws)])
    pm.inventorize_permissions()
    ws.groups.delete(group.id)
    state = GroupMigrationState()
    pm.apply_group_permissions(state, "backup")
