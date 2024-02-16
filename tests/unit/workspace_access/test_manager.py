import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.mixins.sql import Row
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager, Permissions

from ..framework.mocks import MockBackend


@pytest.fixture
def b():
    return MockBackend()


def test_inventory_table_manager_init(b):
    pi = PermissionManager(b, "test_database", [])

    assert pi._full_name == "hive_metastore.test_database.permissions"


def test_cleanup(b):
    pi = PermissionManager(b, "test_database", [])

    pi.cleanup()

    assert b.queries[0] == "DROP TABLE IF EXISTS hive_metastore.test_database.permissions"


def test_save(b):
    pi = PermissionManager(b, "test_database", [])

    pi._save([Permissions("object1", "clusters", "test acl")])

    assert [Permissions(object_id="object1", object_type="clusters", raw="test acl")] == b.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


def permissions_row(*data):
    row = Row(data)
    row.__columns__ = ["object_id", "object_type", "raw"]
    return row


def make_row(data, columns):
    row = Row(data)
    row.__columns__ = columns
    return row


def test_load_all():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                permissions_row("object1", "clusters", "test acl"),
            ],
            "SELECT COUNT": [
                make_row([12], ["cnt"]),
            ],
        }
    )
    pi = PermissionManager(sql_backend, "test_database", [])

    output = pi.load_all()
    assert output[0] == Permissions("object1", "clusters", "test acl")


def test_load_all_no_rows_present():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                permissions_row("object1", "clusters", "test acl"),
            ],
            "SELECT COUNT": [
                make_row([0], ["cnt"]),
            ],
        }
    )

    pi = PermissionManager(sql_backend, "test_database", [])

    with pytest.raises(RuntimeError):
        pi.load_all()


def test_manager_inventorize(b, mocker):
    some_crawler = mocker.Mock()
    some_crawler.get_crawler_tasks = lambda: [lambda: None, lambda: Permissions("a", "b", "c"), lambda: None]
    pm = PermissionManager(b, "test_database", [some_crawler])

    pm.inventorize_permissions()

    assert [Permissions(object_id="a", object_type="b", raw="c")] == b.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


def test_manager_apply(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
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
            ],
            "SELECT COUNT": [
                make_row([12], ["cnt"]),
            ],
        }
    )

    # has to be set, as it's going to be appended through multiple threads
    applied_items = set()
    mock_applier = mocker.Mock()
    mock_applier.object_types = lambda: {"clusters", "cluster-policies"}
    # this emulates a real applier and call to an API
    mock_applier.get_apply_task = lambda item, _: lambda: applied_items.add(f"{item.object_id} {item.object_id}")

    pm = PermissionManager(sql_backend, "test_database", [mock_applier])
    group_migration_state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace=None,
                name_in_workspace="group",
                name_in_account="group",
                temporary_name="group_backup",
                members=None,
                entitlements=None,
                external_id=None,
                roles=None,
            )
        ]
    )

    pm.apply_group_permissions(group_migration_state)

    assert {"test2 test2", "test test"} == applied_items


def test_unregistered_support():
    sql_backend = MockBackend(
        rows={
            "SELECT": [
                permissions_row("test", "__unknown__", "{}"),
            ]
        }
    )
    pm = PermissionManager(sql_backend, "test", [])
    pm.apply_group_permissions(migration_state=MagicMock())


def test_factory(mocker):
    ws = mocker.Mock()
    ws.groups.list.return_value = []
    sql_backend = MockBackend()
    permission_manager = PermissionManager.factory(ws, sql_backend, "test")
    appliers = permission_manager._appliers()

    assert sorted(
        {
            "sql/warehouses",
            "registered-models",
            "instance-pools",
            "jobs",
            "directories",
            "experiments",
            "clusters",
            "notebooks",
            "repos",
            "files",
            "authorization",
            "pipelines",
            "cluster-policies",
            "dashboards",
            "queries",
            "alerts",
            "secrets",
            "entitlements",
            "roles",
            "ANY FILE",
            "FUNCTION",
            "ANONYMOUS FUNCTION",
            "CATALOG",
            "TABLE",
            "VIEW",
            "DATABASE",
        }
    ) == sorted(appliers.keys())


def test_manager_verify(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
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
            ],
            "SELECT COUNT": [
                make_row([12], ["cnt"]),
            ],
        }
    )

    # has to be set, as it's going to be appended through multiple threads
    items = set()
    mock_verifier = mocker.Mock()
    mock_verifier.object_types = lambda: {"clusters"}
    # this emulates a real verifier and call to an API
    mock_verifier.get_verify_task = lambda item: lambda: items.add(f"{item.object_id} {item.object_id}")

    pm = PermissionManager(sql_backend, "test_database", [mock_verifier])
    result = pm.verify_group_permissions()

    assert result
    assert {"test test"} == items


def test_manager_verify_not_supported_type(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
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
            ],
            "SELECT COUNT": [
                make_row([12], ["cnt"]),
            ],
        }
    )

    mock_verifier = mocker.Mock()
    mock_verifier.object_types = lambda: {"not_supported"}
    pm = PermissionManager(sql_backend, "test_database", [mock_verifier])

    with pytest.raises(ValueError):
        pm.verify_group_permissions()


def test_manager_verify_no_tasks(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
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
            ],
            "SELECT COUNT": [
                make_row([12], ["cnt"]),
            ],
        }
    )

    mock_verifier = mocker.Mock()
    mock_verifier.object_types = lambda: {"clusters"}
    # this emulates a real verifier and call to an API
    mock_verifier.get_verify_task = lambda item: None

    pm = PermissionManager(sql_backend, "test_database", [mock_verifier])
    result = pm.verify_group_permissions()

    assert result
