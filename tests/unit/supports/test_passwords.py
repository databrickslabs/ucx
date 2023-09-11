import json
from unittest.mock import MagicMock

from databricks.sdk.service import iam

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem, Supports
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.supports.impl import get_supports
from databricks.labs.ucx.supports.passwords import PasswordsSupport


def test_in_dict():
    all_supports = get_supports(ws=MagicMock(), workspace_start_path="/", num_threads=1)
    assert Supports.passwords in all_supports
    assert all_supports[Supports.passwords].__class__.__name__ == "PasswordsSupport"


def test_crawlers():
    ws_mock = MagicMock()
    ws_mock.users.get_password_permissions.return_value = iam.PasswordPermissions(
        access_control_list=[
            iam.PasswordAccessControlResponse(
                all_permissions=[
                    iam.PasswordPermission(inherited=False, permission_level=iam.PasswordPermissionLevel.CAN_USE)
                ],
            )
        ]
    )
    pwd = PasswordsSupport(ws=ws_mock)
    tasks = pwd.get_crawler_tasks()
    assert len(tasks) == 1
    getter = tasks[0]
    getter()
    ws_mock.users.get_password_permissions.assert_called_once()


def test_apply():
    ws_mock = MagicMock()
    pwd = PasswordsSupport(ws=ws_mock)
    sample_permissions = iam.PasswordPermissions(
        access_control_list=[
            iam.PasswordAccessControlResponse(
                all_permissions=[
                    iam.PasswordPermission(inherited=False, permission_level=iam.PasswordPermissionLevel.CAN_USE)
                ],
                group_name="test",
            ),
            iam.PasswordAccessControlResponse(
                all_permissions=[
                    iam.PasswordPermission(inherited=True, permission_level=iam.PasswordPermissionLevel.CAN_USE)
                ],
                group_name="test2",
            ),
            iam.PasswordAccessControlResponse(
                all_permissions=[
                    iam.PasswordPermission(inherited=False, permission_level=iam.PasswordPermissionLevel.CAN_USE)
                ],
                group_name="test3",
            ),
            iam.PasswordAccessControlResponse(
                all_permissions=[
                    iam.PasswordPermission(inherited=True, permission_level=iam.PasswordPermissionLevel.CAN_USE)
                ],
                group_name="test4",
            ),
        ]
    )

    sample_item = PermissionsInventoryItem(
        object_id="test", support=Supports.passwords, raw_object_permissions=json.dumps(sample_permissions.as_dict())
    )
    ms = GroupMigrationState()
    ms.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name="test"),
            backup=iam.Group(display_name="db-temp-test"),
            account=iam.Group(display_name="test"),
        )
    )

    ms.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name="test2"),
            backup=iam.Group(display_name="db-temp-test2"),
            account=iam.Group(display_name="test2"),
        )
    )

    task = pwd.get_apply_task(item=sample_item, migration_state=ms, destination="backup")
    task()

    expected_requests: list[iam.PasswordAccessControlRequest] = [
        iam.PasswordAccessControlRequest(
            group_name="db-temp-test",
            permission_level=iam.PasswordPermissionLevel.CAN_USE,
        ),
        iam.PasswordAccessControlRequest(
            group_name="test3",
            permission_level=iam.PasswordPermissionLevel.CAN_USE,
        ),
    ]

    ws_mock.users.set_password_permissions.assert_called_once_with(expected_requests)
