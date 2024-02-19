from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.manager import PermissionManager


def test_permissions_save_and_load(ws, sql_backend, inventory_schema, env_or_skip):
    permission_manager = PermissionManager(sql_backend, inventory_schema, [])

    saved = [
        Permissions(object_id="abc", object_type="bcd", raw="def"),
        Permissions(object_id="efg", object_type="fgh", raw="ghi"),
    ]

    permission_manager._save(saved)  # pylint: disable=protected-access
    loaded = permission_manager.load_all()

    assert saved == loaded
