import os

from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.tacl._internal import StatementExecutionBackend


def test_permissions_save_and_load(ws, make_schema):
    schema = make_schema().split(".")[-1]
    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])
    pi = PermissionsInventoryTable(backend, schema)

    saved = [
        PermissionsInventoryItem(object_id="abc", support="bcd", raw_object_permissions="def"),
        PermissionsInventoryItem(object_id="efg", support="fgh", raw_object_permissions="ghi"),
    ]

    pi.save(saved)
    loaded = pi.load_all()

    assert saved == loaded
