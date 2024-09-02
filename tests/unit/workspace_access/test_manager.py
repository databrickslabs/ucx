import json
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service import iam

from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.ucx.workspace_access.base import AclSupport
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager, Permissions


@pytest.fixture
def mock_backend():
    return MockBackend()


def test_inventory_table_manager_init(mock_backend):
    permission_manager = PermissionManager(mock_backend, "test_database", [])

    assert permission_manager.full_name == "hive_metastore.test_database.permissions"


def test_cleanup(mock_backend):
    permission_manager = PermissionManager(mock_backend, "test_database", [])

    permission_manager.cleanup()

    assert mock_backend.queries[0] == "DROP TABLE IF EXISTS `hive_metastore`.`test_database`.`permissions`"


def test_save(mock_backend):
    permission_manager = PermissionManager(mock_backend, "test_database", [])

    permission_manager._save([Permissions("object1", "clusters", "test acl")])  # pylint: disable=protected-access

    assert [Row(object_id="object1", object_type="clusters", raw="test acl")] == mock_backend.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


_PermissionsRow = Row.factory(["object_id", "object_type", "raw"])


def test_load_all():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                _PermissionsRow("object1", "clusters", "test acl"),
            ],
            "SELECT COUNT": [Row(cnt=12)],
        }
    )
    permission_manager = PermissionManager(sql_backend, "test_database", [])

    output = permission_manager.load_all()
    assert output[0] == Permissions(object_id="object1", object_type="clusters", raw="test acl")


def test_load_all_no_rows_present():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                _PermissionsRow("object1", "clusters", "test acl"),
            ],
            "SELECT COUNT": [Row(cnt=0)],
        }
    )

    permission_manager = PermissionManager(sql_backend, "test_database", [])

    with pytest.raises(RuntimeError):
        permission_manager.load_all()


def test_manager_inventorize(mock_backend, mocker):
    some_crawler = mocker.Mock()
    some_crawler.get_crawler_tasks = lambda: [lambda: None, lambda: Permissions("a", "b", "c"), lambda: None]
    permission_manager = PermissionManager(mock_backend, "test_database", [some_crawler])

    permission_manager.inventorize_permissions()

    assert [Row(object_id="a", object_type="b", raw="c")] == mock_backend.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


def test_manager_inventorize_ignore_error(mock_backend, mocker):
    def raise_error():
        raise DatabricksError(
            "Model serving is not enabled for your shard. "
            "Please contact your organization admin or Databricks support.",
            error_code="FEATURE_DISABLED",
        )

    some_crawler = mocker.Mock()
    some_crawler.get_crawler_tasks = lambda: [lambda: None, lambda: Permissions("a", "b", "c"), raise_error]
    permission_manager = PermissionManager(mock_backend, "test_database", [some_crawler])

    permission_manager.inventorize_permissions()

    assert [Row(object_id="a", object_type="b", raw="c")] == mock_backend.rows_written_for(
        "hive_metastore.test_database.permissions", "append"
    )


def test_manager_inventorize_fail_with_error(mock_backend, mocker):
    def raise_error():
        raise DatabricksError(
            "Fail the job",
            error_code="NO_SKIP",
        )

    def raise_error_no_code():
        raise TimeoutError

    some_crawler = mocker.Mock()
    some_crawler.get_crawler_tasks = lambda: [lambda: Permissions("a", "b", "c"), raise_error, raise_error_no_code]
    permission_manager = PermissionManager(mock_backend, "test_database", [some_crawler])

    with pytest.raises(ManyError) as expected_err:
        permission_manager.inventorize_permissions()
    assert len(expected_err.value.errs) == 2


def test_manager_apply(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                _PermissionsRow(
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
                _PermissionsRow(
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
            "SELECT COUNT": [Row(cnt=12)],
        }
    )

    # has to be set, as it's going to be appended through multiple threads
    applied_items = set()
    mock_applier = mocker.Mock()
    mock_applier.object_types = lambda: {"clusters", "cluster-policies"}
    # this emulates a real applier and call to an API
    mock_applier.get_apply_task = lambda item, _: lambda: applied_items.add(f"{item.object_id} {item.object_id}")

    permission_manager = PermissionManager(sql_backend, "test_database", [mock_applier])
    group_migration_state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="",
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

    permission_manager.apply_group_permissions(group_migration_state)

    assert {"test2 test2", "test test"} == applied_items


def test_unregistered_support():
    sql_backend = MockBackend(
        rows={
            "SELECT": [
                _PermissionsRow("test", "__unknown__", "{}"),
            ]
        }
    )
    permission_manager = PermissionManager(sql_backend, "test", [])
    permission_manager.apply_group_permissions(migration_state=MigrationState([]))


def test_manager_verify():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                _PermissionsRow(
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
            "SELECT COUNT": [Row(cnt=12)],
        }
    )

    # has to be set, as it's going to be appended through multiple threads
    items = set()
    mock_verifier = create_autospec(AclSupport)  # pylint: disable=mock-no-usage
    mock_verifier.object_types = lambda: {"clusters"}
    # this emulates a real verifier and call to an API
    mock_verifier.get_verify_task = lambda item: lambda: items.add(f"{item.object_id} {item.object_id}")

    permission_manager = PermissionManager(sql_backend, "test_database", [mock_verifier])
    result = permission_manager.verify_group_permissions()

    assert result
    assert {"test test"} == items


def test_manager_verify_not_supported_type():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                _PermissionsRow(
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
            "SELECT COUNT": [Row(cnt=12)],
        }
    )

    mock_verifier = create_autospec(AclSupport)  # pylint: disable=mock-no-usage
    mock_verifier.object_types = lambda: {"not_supported"}
    permission_manager = PermissionManager(sql_backend, "test_database", [mock_verifier])

    with pytest.raises(ValueError):
        permission_manager.verify_group_permissions()


def test_manager_verify_no_tasks():
    sql_backend = MockBackend(
        rows={
            "SELECT object_id": [
                _PermissionsRow(
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
            "SELECT COUNT": [Row(cnt=12)],
        }
    )

    mock_verifier = create_autospec(AclSupport)  # pylint: disable=mock-no-usage
    mock_verifier.object_types = lambda: {"clusters"}
    # this emulates a real verifier and call to an API
    mock_verifier.get_verify_task = lambda item: None

    permission_manager = PermissionManager(sql_backend, "test_database", [mock_verifier])
    result = permission_manager.verify_group_permissions()

    assert result


def test_manager_apply_experimental_no_tasks(caplog):
    ws = create_autospec(WorkspaceClient)
    group_migration_state = MigrationState([])

    with caplog.at_level("INFO"):
        group_migration_state.apply_to_groups_with_different_names(ws)
        assert "No valid groups selected, nothing to do." in caplog.messages
    ws.permission_migration.migrate_permissions.assert_not_called()
