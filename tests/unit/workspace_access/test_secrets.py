import json
from datetime import timedelta
from unittest.mock import MagicMock, call

import pytest
from databricks.sdk.service import workspace

from databricks.labs.ucx.workspace_access.groups import MigrationState
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
    assert item.raw == '[{"permission": "MANAGE", "principal": "test"}]'


def test_secret_scopes_apply(migration_state: MigrationState):
    ws = MagicMock()
    sup = SecretScopesSupport(ws=ws)
    item = Permissions(
        object_id="test",
        object_type="secrets",
        raw=json.dumps(
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
            principal="test",
            permission=workspace.AclPermission.MANAGE,
        ),
        workspace.AclItem(
            principal="irrelevant",
            permission=workspace.AclPermission.MANAGE,
        ),
    ]

    task = sup.get_apply_task(item, migration_state)
    task()
    assert ws.secrets.put_acl.call_count == 2

    calls = [
        call("test", "test", workspace.AclPermission.MANAGE),
        call("test", "irrelevant", workspace.AclPermission.MANAGE),
    ]
    ws.secrets.put_acl.assert_has_calls(calls, any_order=False)


def test_secret_scopes_apply_failed():
    ws = MagicMock()
    sup = SecretScopesSupport(ws, timedelta(seconds=1))
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(TimeoutError) as e:
        sup._rate_limited_put_acl("test", "db-temp-test", expected_permission)
    assert "Timed out after" in str(e.value)


def test_secret_scopes_apply_incorrect():
    ws = MagicMock()
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="db-temp-test",
            permission=workspace.AclPermission.READ,
        )
    ]

    sup = SecretScopesSupport(ws, timedelta(seconds=1))
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(TimeoutError):
        sup._rate_limited_put_acl("test", "db-temp-test", expected_permission)


def test_secret_scopes_reapply():
    ws = MagicMock()
    ws.secrets.list_acls.side_effect = [
        [
            workspace.AclItem(
                principal="db-temp-test",
                permission=workspace.AclPermission.READ,
            )
        ],
        [
            workspace.AclItem(
                principal="db-temp-test",
                permission=workspace.AclPermission.MANAGE,
            )
        ],
    ]

    sup = SecretScopesSupport(ws, timedelta(seconds=10))
    expected_permission = workspace.AclPermission.MANAGE

    sup._rate_limited_put_acl("test", "db-temp-test", expected_permission)
    assert ws.secrets.put_acl.call_count == 2


def test_secret_scopes_reapply_check_valueerror():
    ws = MagicMock()
    ws.secrets.list_acls.side_effect = [
        [
            workspace.AclItem(
                principal="db-temp-test",
                permission=workspace.AclPermission.READ,
            )
        ]
    ]

    sup = SecretScopesSupport(ws, timedelta(seconds=10))
    expected_permission = workspace.AclPermission.MANAGE

    with pytest.raises(ValueError):
        sup._reapply_on_failure("test", "db-temp-test", expected_permission)


def test_secret_scopes_reapply_check_exception_type():
    ws = MagicMock()
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="db-temp-test",
            permission=workspace.AclPermission.READ,
        )
    ]

    sup = SecretScopesSupport(ws, timedelta(seconds=1))
    expected_permission = workspace.AclPermission.MANAGE
    try:
        sup._rate_limited_put_acl("test", "db-temp-test", expected_permission)
    except Exception as e:
        assert isinstance(e, TimeoutError)
