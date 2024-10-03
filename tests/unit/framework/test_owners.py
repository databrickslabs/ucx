import re
from collections.abc import Callable, Sequence

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam

from databricks.labs.ucx.framework.owners import Ownership, Record


class _OwnershipFixture(Ownership[Record]):
    def __init__(
        self,
        ws: WorkspaceClient,
        *,
        owner_fn: Callable[[Record], str | None] = lambda _: None,
    ):
        super().__init__(ws)
        self._owner_fn = owner_fn

    def _get_owner(self, record: Record) -> str | None:
        return self._owner_fn(record)


def _setup_accounts(
    ws,
    *,
    account_users: Sequence[iam.User] = (),
    workspace_users: Sequence[iam.User] = (),
    groups: Sequence[iam.Group] = (),
) -> None:
    # Stub for the workspace users.
    ws.users.list.return_value = list(workspace_users)

    # Stub for the groups.
    groups_by_id = {group.id: group for group in groups}

    def stub_groups_get(group_id: str) -> iam.Group:
        try:
            return groups_by_id[group_id]
        except KeyError as e:
            msg = f"Group not found: {group_id}"
            raise NotFound(msg) from e

    ws.groups.get.side_effect = stub_groups_get
    ws.groups.list.return_value = groups

    # Stub for the account users.
    def stub_rest_call(method: str, path: str | None = None, query: dict | None = None) -> dict:
        if method == "GET" and path == "/api/2.0/account/scim/v2/Users" and query:
            return {"Resources": [user.as_dict() for user in account_users]}
        msg = f"Call not mocked: {method} {path}"
        raise NotImplementedError(msg)

    ws.api_client.do.side_effect = stub_rest_call


def _create_workspace_admin(user_name: str, admins_group_id: str) -> iam.User:
    return iam.User(
        user_name=user_name,
        active=True,
        groups=[iam.ComplexValue(display="admins", ref=f"Groups/{admins_group_id}", value=admins_group_id)],
    )


def _create_account_admin(user_name: str) -> iam.User:
    return iam.User(user_name=user_name, active=True, roles=[iam.ComplexValue(value="account_admin")])


def _create_workspace_group(display_name: str, group_id: str) -> iam.Group:
    return iam.Group(display_name=display_name, id=group_id, meta=iam.ResourceMeta(resource_type="WorkspaceGroup"))


@pytest.fixture(autouse=True)
def _clear_ownership_cache() -> None:
    """Ensure that the class-level cache of workspace owners is cleared before each test."""
    Ownership.reset_cache()


def test_ownership_prefers_record_owner(ws) -> None:
    """Verify that if an owner for the record can be found, that is used."""
    ownership = _OwnershipFixture[str](ws, owner_fn=lambda _: "bob")
    owner = ownership.owner_of("school")

    assert owner == "bob"
    ws.get_workspace_id.assert_not_called()


def test_ownership_admin_user_fallback(ws) -> None:
    """Verify that if no owner for the record can be found, an admin user is returned instead."""
    _setup_accounts(ws, account_users=[_create_account_admin("jane")])

    ownership = _OwnershipFixture[str](ws)
    owner = ownership.owner_of("school")

    assert owner == "jane"


def test_ownership_workspace_admin_preferred_over_account_admin(ws) -> None:
    """Verify that when both workspace and account administrators are configured, the workspace admin is preferred."""
    admins_group = _create_workspace_group("admins", group_id="1")
    assert admins_group.id
    workspace_users = [_create_workspace_admin("bob", admins_group_id=admins_group.id)]
    account_users = [_create_account_admin("jane")]
    _setup_accounts(ws, account_users=account_users, workspace_users=workspace_users, groups=[admins_group])

    ownership = _OwnershipFixture[str](ws)
    owner = ownership.owner_of("school")

    assert owner == "bob"


def test_ownership_admin_ignore_inactive(ws) -> None:
    """Verify that inactive workspace administrators are ignored when locating an administrator."""
    admins_group = _create_workspace_group("admins", group_id="1")
    assert admins_group.id
    bob = _create_workspace_admin("bob", admins_group_id=admins_group.id)
    bob.active = False
    jane = _create_account_admin("jane")
    jane.active = False
    _setup_accounts(ws, account_users=[jane], workspace_users=[bob], groups=[admins_group])

    ownership = _OwnershipFixture[str](ws)
    # All admins are inactive, so an exception should be raised.
    with pytest.raises(RuntimeError, match="No active workspace or account administrator"):
        _ = ownership.owner_of("school")


def test_ownership_workspace_admin_prefer_first_alphabetically(ws) -> None:
    """Verify that when multiple workspace administrators can found, the first alphabetically is used."""
    admins_group = _create_workspace_group("admins", group_id="1")
    assert admins_group.id
    workspace_users = [
        _create_workspace_admin("bob", admins_group_id=admins_group.id),
        _create_workspace_admin("andrew", admins_group_id=admins_group.id),
        _create_workspace_admin("jane", admins_group_id=admins_group.id),
    ]
    _setup_accounts(ws, workspace_users=workspace_users, groups=[admins_group])

    ownership = _OwnershipFixture[str](ws)
    owner = ownership.owner_of("school")

    assert owner == "andrew"


def test_ownership_account_admin_prefer_first_alphabetically(ws) -> None:
    """Verify that when multiple account administrators can found, the first alphabetically preferred is used."""
    account_users = [
        _create_account_admin("bob"),
        _create_account_admin("andrew"),
        _create_account_admin("jane"),
    ]
    _setup_accounts(ws, account_users=account_users)

    ownership = _OwnershipFixture[str](ws)
    owner = ownership.owner_of("school")

    assert owner == "andrew"


def test_ownership_error_when_no_owner_can_be_located(ws) -> None:
    """Verify that an error is raised when no workspace or account administrators can be found."""
    _setup_accounts(ws)

    ownership = _OwnershipFixture[str](ws)
    # No admins.
    workspace_id = ws.get_workspace_id()
    expected_message = f"No active workspace or account administrator can be found for workspace: {workspace_id}"
    with pytest.raises(RuntimeError, match=re.escape(expected_message)):
        _ = ownership.owner_of("school")


def test_ownership_fallback_instance_cache(ws) -> None:
    """Verify that the fallback owner is cached on each instance to avoid many REST calls."""
    pytest.xfail("Not yet implemented")


def test_ownership_fallback_class_cache(ws) -> None:
    """Verify that the fallback owner for a workspace is cached at class level to avoid many REST calls."""
    pytest.xfail("Not yet implemented")


def test_ownership_fallback_class_cache_multiple_workspaces(ws) -> None:
    """Verify that cache of workspace administrators supports multiple workspaces."""
    pytest.xfail("Not yet implemented")


def test_ownership_fallback_error_handling(ws) -> None:
    """Verify that the class-level owner-cache and tracks errors to avoid many REST calls."""
    pytest.xfail("Not yet implemented")
