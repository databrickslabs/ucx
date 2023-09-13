from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.providers.mixins.sql import Row

from .mocks import MockBackend


def test_inventory_table_manager_init():
    b = MockBackend()
    pi = PermissionsInventoryTable(b, "test_database")

    assert pi._full_name == "hive_metastore.test_database.permissions"


def test_cleanup():
    b = MockBackend()
    pi = PermissionsInventoryTable(b, "test_database")

    pi.cleanup()

    assert "DROP TABLE IF EXISTS hive_metastore.test_database.permissions" == b.queries[0]


def test_save():
    b = MockBackend()
    pi = PermissionsInventoryTable(b, "test_database")

    pi.save([PermissionsInventoryItem("object1", "clusters", "test acl")])

    assert (
        "INSERT INTO hive_metastore.test_database.permissions (object_id, support, "
        "raw_object_permissions) VALUES ('object1', 'clusters', 'test acl')"
    ) == b.queries[0]


def make_row(data, columns):
    row = Row(data)
    row.__columns__ = columns
    return row


def test_load_all():
    b = MockBackend(
        rows={
            "SELECT": [
                make_row(("object1", "clusters", "test acl"), ["object_id", "support", "raw_object_permissions"]),
            ]
        }
    )
    pi = PermissionsInventoryTable(b, "test_database")

    output = pi.load_all()
    assert output[0] == PermissionsInventoryItem("object1", support="clusters", raw_object_permissions="test acl")
