from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.manager import PermissionManager


def test_permissions_save_and_load(ws, sql_backend, inventory_schema, env_or_skip):
    pi = PermissionManager(sql_backend, inventory_schema, [])

    saved = [
        Permissions(object_id="abc", object_type="bcd", raw="def"),
        Permissions(object_id="efg", object_type="fgh", raw="ghi"),
    ]

    pi._save(saved)
    loaded = pi.load_all()

    assert saved == loaded
