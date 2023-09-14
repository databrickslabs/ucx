import json
from unittest import mock
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import iam
from unit.framework.mocks import MockBackend

from databricks.labs.ucx.mixins.sql import Row
from databricks.labs.ucx.workspace_access.manager import PermissionManager, Permissions


@pytest.fixture
def b():
    return MockBackend()


def test_inventory_table_manager_init(b):
    pi = PermissionManager(b, "test_database", [], {})

    assert pi._full_name == "hive_metastore.test_database.permissions"


def test_cleanup(b):
    pi = PermissionManager(b, "test_database", [], {})

    pi.cleanup()

    assert "DROP TABLE IF EXISTS hive_metastore.test_database.permissions" == b.queries[0]


def test_save(b):
    pi = PermissionManager(b, "test_database", [], {})

    pi.save([Permissions("object1", "clusters", "test acl")])

    assert (
        "INSERT INTO hive_metastore.test_database.permissions (object_id, object_type, "
        "raw_object_permissions) VALUES ('object1', 'clusters', 'test acl')"
    ) == b.queries[0]


def permissions_row(*data):
    row = Row(data)
    row.__columns__ = ["object_id", "object_type", "raw_object_permissions"]
    return row


def test_load_all():
    b = MockBackend(
        rows={
            "SELECT": [
                permissions_row("object1", "clusters", "test acl"),
            ]
        }
    )
    pi = PermissionManager(b, "test_database", [], {})

    output = pi.load_all()
    assert output[0] == Permissions("object1", "clusters", "test acl")


def test_manager_inventorize(b):
    # FIXME: make it work
    pm = PermissionManager(b, "test_database", [], {})

    with mock.patch("databricks.labs.ucx.inventory.permissions.ThreadedExecution.run", MagicMock()) as run_mock:
        pm.inventorize_permissions()
        run_mock.assert_called_once()


def test_manager_apply(mocker):
    b = MockBackend(
        rows={
            "SELECT": [
                permissions_row(
                    "test",
                    "clusters",
                    json.dumps(
                        iam.ObjectPermissions(
                            object_id="test",
                            object_type="clusters",
                            access_control_list=[
                                iam.AccessControlResponse(
                                    group_name="test",
                                    all_permissions=[
                                        iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)
                                    ],
                                )
                            ],
                        ).as_dict()
                    ),
                ),
                permissions_row(
                    "test2",
                    "cluster-policies",
                    json.dumps(
                        iam.ObjectPermissions(
                            object_id="test",
                            object_type="cluster-policies",
                            access_control_list=[
                                iam.AccessControlResponse(
                                    group_name="test",
                                    all_permissions=[
                                        iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)
                                    ],
                                )
                            ],
                        ).as_dict()
                    ),
                ),
            ]
        }
    )
    clusters_applier = mocker.Mock()
    cluster_policy_applier = mocker.Mock()
    other_applier = mocker.Mock()
    pm = PermissionManager(
        b,
        "test_database",
        [],
        {
            "clusters": clusters_applier,
            "cluster-policies": cluster_policy_applier,
            "other": other_applier,
        },
    )
    pm.apply_group_permissions(MagicMock(), "backup")

    clusters_applier.assert_called_with(...)
    cluster_policy_applier.assert_called_with(...)


def test_unregistered_support():
    b = MockBackend(
        rows={
            "SELECT": [
                permissions_row("test", "__unknown__", "{}"),
            ]
        }
    )
    pm = PermissionManager(b, "test", [], {})
    with pytest.raises(ValueError):
        pm.apply_group_permissions(migration_state=MagicMock(), destination="backup")
