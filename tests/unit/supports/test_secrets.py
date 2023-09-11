import json
from unittest.mock import MagicMock, call

from databricks.sdk.service import iam, workspace

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.support.secrets import SecretScopesSupport


def test_secret_scopes_crawler():
    ws = MagicMock()
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="test",
            permission=workspace.AclPermission.MANAGE,
        )
    ]
    ws.secrets.list_scopes.return_value = [
        workspace.SecretScope(
            name="test",
        )
    ]

    sup = SecretScopesSupport(ws=ws)
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.secrets.list_scopes.assert_called_once()

    _task = tasks[0]
    item = _task()

    assert item.object_id == "test"
    assert item.support == "secrets"
    assert item.raw_object_permissions == '[{"permission": "MANAGE", "principal": "test"}]'


def test_secret_scopes_apply():
    ws = MagicMock()
    sup = SecretScopesSupport(ws=ws)
    item = PermissionsInventoryItem(
        object_id="test",
        support="secrets",
        raw_object_permissions=json.dumps(
            [
                workspace.AclItem(
                    principal="test",
                    permission=workspace.AclPermission.MANAGE,
                ).as_dict(),
                workspace.AclItem(
                    principal="irrelevant",
                    permission=workspace.AclPermission.MANAGE,
                ).as_dict(),
            ]
        ),
    )
    ms = GroupMigrationState()
    ms.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name="test"),
            backup=iam.Group(display_name="db-temp-test"),
            account=iam.Group(display_name="test", id="test-acc"),
        )
    )
    task = sup.get_apply_task(item, ms, "backup")
    task()
    assert ws.secrets.put_acl.call_count == 2

    calls = [
        call("test", "db-temp-test", workspace.AclPermission.MANAGE),
        call("test", "irrelevant", workspace.AclPermission.MANAGE),
    ]
    ws.secrets.put_acl.assert_has_calls(calls, any_order=False)
