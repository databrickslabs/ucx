import json
import logging
from collections.abc import Iterator
from datetime import timedelta
from unittest.mock import call, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.errors import ResourceDoesNotExist

from databricks.labs.ucx.workspace_access.groups import MigrationState
from databricks.labs.ucx.workspace_access.secrets import (
    Permissions,
    SecretScopesSupport,
)

# pylint: disable=protected-access


def test_secret_scopes_crawler():
    ws = create_autospec(WorkspaceClient)
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


def test_secret_scopes_disappearing_during_crawl(ws, caplog) -> None:
    """Verify that when crawling secret scopes we continue instead of failing when a secret scope disappears."""

    ws.secrets.list_scopes.return_value = [
        workspace.SecretScope(name="will_remain"),
        workspace.SecretScope(name="will_disappear"),
    ]

    def mock_list_acls(scope: str) -> Iterator[workspace.AclItem]:
        if scope == "will_disappear":
            raise ResourceDoesNotExist("Simulated disappearance")
        yield workspace.AclItem(principal="a_principal", permission=workspace.AclPermission.MANAGE)

    ws.secrets.list_acls = mock_list_acls

    sup = SecretScopesSupport(ws=ws)

    tasks = list(sup.get_crawler_tasks())
    with caplog.at_level(logging.WARNING):
        task_results = [task() for task in tasks]

    assert task_results == [
        Permissions(
            object_id="will_remain", object_type="secrets", raw='[{"permission": "MANAGE", "principal": "a_principal"}]'
        ),
        None,
    ]
    assert "Secret scope disappeared, cannot assess: will_disappear" in caplog.messages


def test_secret_scopes_crawler_include():
    ws = create_autospec(WorkspaceClient)
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="test",
            permission=workspace.AclPermission.MANAGE,
        )
    ]

    sup = SecretScopesSupport(ws=ws, include_object_permissions=["secrets:included"])
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.secrets.list_scopes.assert_not_called()

    _task = tasks[0]
    item = _task()

    assert item.object_id == "included"
    assert item.object_type == "secrets"
    assert item.raw == '[{"permission": "MANAGE", "principal": "test"}]'


def test_secret_scopes_apply(migration_state: MigrationState):
    ws = create_autospec(WorkspaceClient)
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
    ws = create_autospec(WorkspaceClient)
    sup = SecretScopesSupport(ws, timedelta(seconds=1))
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(TimeoutError) as e:
        sup._applier_task("test", "db-temp-test", expected_permission)
    assert "Timed out after" in str(e.value)
    ws.secrets.put_acl.assert_called()


def test_secret_scopes_apply_incorrect():
    ws = create_autospec(WorkspaceClient)
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="db-temp-test",
            permission=workspace.AclPermission.READ,
        )
    ]

    sup = SecretScopesSupport(ws, timedelta(seconds=1))
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(TimeoutError):
        sup._applier_task("test", "db-temp-test", expected_permission)


def test_secret_scopes_reapply():
    ws = create_autospec(WorkspaceClient)
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

    sup._applier_task("test", "db-temp-test", expected_permission)
    assert ws.secrets.put_acl.call_count == 2


def test_secret_scopes_reapply_check_valueerror():
    ws = create_autospec(WorkspaceClient)
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
    ws = create_autospec(WorkspaceClient)
    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="db-temp-test",
            permission=workspace.AclPermission.READ,
        )
    ]

    sup = SecretScopesSupport(ws, timedelta(seconds=1))
    expected_permission = workspace.AclPermission.MANAGE
    with pytest.raises(TimeoutError):
        sup._applier_task("test", "db-temp-test", expected_permission)


def test_verify_task_should_return_true_if_permissions_applied():
    ws = create_autospec(WorkspaceClient)
    sup = SecretScopesSupport(ws=ws)
    item = Permissions(
        object_id="test",
        object_type="secrets",
        raw=json.dumps(
            [
                workspace.AclItem(
                    principal="test",
                    permission=workspace.AclPermission.MANAGE,
                ).as_dict()
            ]
        ),
    )

    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="test",
            permission=workspace.AclPermission.MANAGE,
        ),
        workspace.AclItem(
            principal="irrelevant",
            permission=workspace.AclPermission.READ,
        ),
    ]

    task = sup.get_verify_task(item)
    result = task()

    assert result


def test_verify_task_should_fail_if_permissions_not_applied():
    ws = create_autospec(WorkspaceClient)
    sup = SecretScopesSupport(ws=ws)
    item = Permissions(
        object_id="test",
        object_type="secrets",
        raw=json.dumps(
            [
                workspace.AclItem(
                    principal="test",
                    permission=workspace.AclPermission.MANAGE,
                ).as_dict()
            ]
        ),
    )

    ws.secrets.list_acls.return_value = [
        workspace.AclItem(
            principal="test",
            permission=workspace.AclPermission.READ,
        )
    ]

    task = sup.get_verify_task(item)

    with pytest.raises(ValueError):
        task()


def test_verify_task_should_fail_if_principal_not_given():
    ws = create_autospec(WorkspaceClient)
    sup = SecretScopesSupport(ws=ws)
    item = Permissions(
        object_id="test",
        object_type="secrets",
        raw=json.dumps(
            [
                workspace.AclItem(
                    principal=None,
                    permission=workspace.AclPermission.MANAGE,
                ).as_dict()
            ]
        ),
    )

    task = sup.get_verify_task(item)

    with pytest.raises(AssertionError):
        task()
    ws.secrets.list_acls.assert_not_called()
    ws.secrets.put_acl.assert_not_called()


def test_verify_task_should_fail_if_permission_not_given():
    ws = create_autospec(WorkspaceClient)
    sup = SecretScopesSupport(ws=ws)
    item = Permissions(
        object_id="test",
        object_type="secrets",
        raw=json.dumps(
            [
                workspace.AclItem(
                    principal="test",
                    permission=None,
                ).as_dict()
            ]
        ),
    )

    task = sup.get_verify_task(item)

    with pytest.raises(AssertionError):
        task()

    ws.secrets.list_acls.assert_not_called()
    ws.secrets.put_acl.assert_not_called()
