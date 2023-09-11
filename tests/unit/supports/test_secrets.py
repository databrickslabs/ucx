from unittest.mock import MagicMock

from databricks.sdk.service import workspace

from databricks.labs.ucx.support.secrets import SecretScopesSupport


def test_secret_scopes_support():
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
    tasks = sup.get_crawler_tasks()
    assert len(tasks) == 1
    ws.secrets.list_scopes.assert_called_once()

    _task = tasks[0]
    item = _task()

    assert item.object_id == "test"
    assert item.support == "secrets"
    assert item.raw_object_permissions == '[{"permission": "MANAGE", "principal": "test"}]'
