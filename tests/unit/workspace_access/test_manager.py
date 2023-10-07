import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group, ResourceMeta

from databricks.labs.ucx.mixins.sql import Row
from databricks.labs.ucx.workspace_access.groups import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.workspace_access.manager import PermissionManager, Permissions

from ..framework.mocks import MockBackend


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

    pi._save([Permissions("object1", "clusters", "test acl")])

    assert [Permissions(object_id="object1", object_type="clusters", raw="test acl")] == b.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


def permissions_row(*data):
    row = Row(data)
    row.__columns__ = ["object_id", "object_type", "raw"]
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

    output = pi._load_all()
    assert output[0] == Permissions("object1", "clusters", "test acl")


def test_manager_inventorize(b, mocker):
    some_crawler = mocker.Mock()
    some_crawler.get_crawler_tasks = lambda: [lambda: None, lambda: Permissions("a", "b", "c"), lambda: None]
    pm = PermissionManager(b, "test_database", [some_crawler], {"b": mocker.Mock()})

    pm.inventorize_permissions()

    assert [Permissions(object_id="a", object_type="b", raw="c")] == b.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


def test_manager_inventorize_unknown_object_type_raises_error(b, mocker):
    some_crawler = mocker.Mock()
    some_crawler.get_crawler_tasks = lambda: [lambda: None, lambda: Permissions("a", "b", "c"), lambda: None]
    pm = PermissionManager(b, "test_database", [some_crawler], {})

    with pytest.raises(KeyError):
        pm.inventorize_permissions()


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

    # has to be set, as it's going to be appended through multiple threads
    applied_items = set()
    mock_applier = mocker.Mock()
    # this emulates a real applier and call to an API
    mock_applier.get_apply_task = lambda item, _, dst: lambda: applied_items.add(
        f"{item.object_id} {item.object_id} {dst}"
    )

    pm = PermissionManager(
        b,
        "test_database",
        [],
        {
            "clusters": mock_applier,
            "cluster-policies": mock_applier,
        },
    )
    group_migration_state: GroupMigrationState = MagicMock()
    group_migration_state.groups = [
        MigrationGroupInfo(
            Group(display_name="group", meta=ResourceMeta(resource_type="WorkspaceGroup")),
            Group(display_name="group_backup", meta=ResourceMeta(resource_type="WorkspaceGroup")),
            Group(display_name="group", meta=ResourceMeta(resource_type="AccountGroup")),
        )
    ]

    pm.apply_group_permissions(group_migration_state, "backup")

    assert {"test2 test2 backup", "test test backup"} == applied_items


def test_unregistered_support():
    b = MockBackend(
        rows={
            "SELECT": [
                permissions_row("test", "__unknown__", "{}"),
            ]
        }
    )
    pm = PermissionManager(b, "test", [], {})
    pm.apply_group_permissions(migration_state=MagicMock(), destination="backup")


def test_factory(mocker):
    ws = mocker.Mock()
    b = MockBackend()
    PermissionManager.factory(ws, b, "test")
