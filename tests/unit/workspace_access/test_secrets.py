import json
from unittest.mock import MagicMock, call

import pytest
from databricks.sdk.service import workspace

from databricks.labs.ucx.workspace_access.groups import GroupMigrationState
from databricks.labs.ucx.workspace_access.secrets import (
    Permissions,
    SecretScopesSupport,
)


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
    assert item.object_type == "secrets"
    assert item.raw_object_permissions == '[{"permission": "MANAGE", "principal": "test"}]'


def test_secret_scopes_apply(migration_state: GroupMigrationState):
    ws = MagicMock()
    sup = SecretScopesSupport(ws=ws)
    item = Permissions(
        object_id="test",
        object_type="secrets",
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

    # positive case - permissions are applied correctly
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="db-temp-test",
            permission=workspace.AclPermission.MANAGE,
        ),
        workspace.AclItem(
            principal="irrelevant",
            permission=workspace.AclPermission.MANAGE,
        ),
    ]

    task = sup.get_apply_task(item, migration_state, "backup")
    task()
    assert ws.secrets.put_acl.call_count == 2

    calls = [
        call("test", "db-temp-test", workspace.AclPermission.MANAGE),
        call("test", "irrelevant", workspace.AclPermission.MANAGE),
    ]
    ws.secrets.put_acl.assert_has_calls(calls, any_order=False)


def test_secret_scopes_apply_failed():
    ws = MagicMock()
    sup = SecretScopesSupport(ws=ws)
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(ValueError) as e:
        sup._inflight_check(
            group_name="db-temp-test", scope_name="test", expected_permission=expected_permission, num_retries=2
        )
        assert "Failed to apply permissions" in str(e.value)


def test_secret_scopes_apply_incorrect():
    ws = MagicMock()
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="db-temp-test",
            permission=workspace.AclPermission.READ,
        )
    ]

    sup = SecretScopesSupport(ws=ws)
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(ValueError) as e:
        sup._inflight_check(
            group_name="db-temp-test", scope_name="test", expected_permission=expected_permission, num_retries=2
        )
        assert "not equal to expected permission" in str(e.value)
