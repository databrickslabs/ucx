from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.manager import PermissionManager


def test_permissions_save_and_load(ws, make_schema, env_or_skip):
    schema = make_schema().split(".")[-1]
    backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    pi = PermissionManager(backend, schema, [], {})

    saved = [
        Permissions(object_id="abc", object_type="bcd", raw="def"),
        Permissions(object_id="efg", object_type="fgh", raw="ghi"),
    ]

    pi._save(saved)
    loaded = pi._load_all()

    assert saved == loaded
