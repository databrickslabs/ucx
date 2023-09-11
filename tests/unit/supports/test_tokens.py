import json
from unittest.mock import MagicMock

from databricks.sdk.service import iam, settings

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem, Supports
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.supports.impl import get_supports
from databricks.labs.ucx.supports.tokens import TokensSupport


def test_in_dict():
    all_supports = get_supports(ws=MagicMock(), workspace_start_path="/", num_threads=1)
    assert Supports.tokens in all_supports
    assert all_supports[Supports.tokens].__class__.__name__ == "TokensSupport"


def test_crawlers():
    ws_mock = MagicMock()
    ws_mock.token_management.get_token_permissions.return_value = settings.TokenPermissions(
        access_control_list=[
            settings.TokenAccessControlResponse(
                all_permissions=[
                    settings.TokenPermission(inherited=False, permission_level=settings.TokenPermissionLevel.CAN_USE)
                ]
            )
        ]
    )
    ts = TokensSupport(ws=ws_mock)
    tasks = ts.get_crawler_tasks()
    assert len(tasks) == 1
    getter = tasks[0]
    item = getter()
    ws_mock.token_management.get_token_permissions.assert_called_once()
    assert item.support == Supports.tokens
    assert item.object_id == "tokens"
    assert isinstance(json.loads(item.raw_object_permissions), dict)


def test_apply():
    ws_mock = MagicMock()
    ts = TokensSupport(ws=ws_mock)
    sample_permissions = settings.TokenPermissions(
        access_control_list=[
            settings.TokenAccessControlResponse(
                all_permissions=[
                    settings.TokenPermission(inherited=False, permission_level=settings.TokenPermissionLevel.CAN_USE)
                ],
                group_name="test",
            ),
            settings.TokenAccessControlResponse(
                all_permissions=[
                    settings.TokenPermission(inherited=True, permission_level=settings.TokenPermissionLevel.CAN_USE)
                ],
                group_name="test2",
            ),
            settings.TokenAccessControlResponse(
                all_permissions=[
                    settings.TokenPermission(inherited=False, permission_level=settings.TokenPermissionLevel.CAN_USE)
                ],
                group_name="test3",
            ),
            settings.TokenAccessControlResponse(
                all_permissions=[
                    settings.TokenPermission(inherited=True, permission_level=settings.TokenPermissionLevel.CAN_USE)
                ],
                group_name="test4",
            ),
        ]
    )
    item = PermissionsInventoryItem(
        object_id="tokens",
        support=Supports.tokens,
        raw_object_permissions=json.dumps(sample_permissions.as_dict()),
    )

    ws_mock.token_management.get_token_permissions.return_value = sample_permissions

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

    expected_requests = [
        settings.TokenAccessControlRequest(
            group_name="db-temp-test", permission_level=settings.TokenPermissionLevel.CAN_USE
        ),
        settings.TokenAccessControlRequest(group_name="test3", permission_level=settings.TokenPermissionLevel.CAN_USE),
    ]

    apply_task = ts.get_apply_task(item, migration_state=ms, destination="backup")
    apply_task()

    ws_mock.token_management.set_token_permissions.assert_called_once_with(expected_requests)
