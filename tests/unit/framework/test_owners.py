import logging
import re
from collections.abc import Callable, Sequence
from unittest.mock import create_autospec, Mock

import pytest
from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from databricks.labs.ucx.framework.owners import (
    AccountAdministratorFinder,
    AdministratorFinder,
    AdministratorLocator,
    Ownership,
    Record,
    WorkspaceAdministratorFinder,
    WorkspacePathOwnership,
)


class _OwnershipFixture(Ownership[Record]):
    def __init__(
        self,
        *,
        owner_fn: Callable[[Record], str | None] = lambda _: None,
    ):
        mock_admin_locator = create_autospec(AdministratorLocator)  # pylint: disable=mock-no-usage
        super().__init__(mock_admin_locator)
        self._owner_fn = owner_fn
        self.mock_admin_locator = mock_admin_locator

    def _maybe_direct_owner(self, record: Record) -> str | None:
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


def test_workspace_admin_finder_active_with_username(ws) -> None:
    """Verify that the workspace admin finder only reports active users with a user-name."""
    admins_group = _create_workspace_group("admins", group_id="1")
    inactive_admin = _create_workspace_admin("inactive_admin_1", admins_group_id="1")
    inactive_admin.active = False
    users = [
        _create_workspace_admin("only_real_admin", admins_group_id="1"),
        inactive_admin,
        _create_workspace_admin("", admins_group_id="1"),
    ]
    _setup_accounts(ws, workspace_users=users, groups=[admins_group])

    finder = WorkspaceAdministratorFinder(ws)
    admins = list(finder.find_admin_users())

    assert [admin.user_name for admin in admins] == ["only_real_admin"]


def test_workspace_admin_finder_admins_members(ws) -> None:
    """Verify that the workspace admin finder only reports members of the 'admins' workspace group."""
    groups = [
        _create_workspace_group("admins", group_id="1"),
        _create_workspace_group("users", group_id="2"),
        _create_workspace_group("not_admins", group_id="3"),
        iam.Group(display_name="admins", id="4", meta=iam.ResourceMeta(resource_type="Group")),
    ]
    users = [
        _create_workspace_admin("admin_1", admins_group_id="1"),
        iam.User(
            user_name="admin_2",
            active=True,
            groups=[
                iam.ComplexValue(display="admins", ref="Groups/1", value="1"),
                iam.ComplexValue(display="users", ref="Groups/2", value="2"),
            ],
        ),
        iam.User(
            user_name="not_admin_1",
            active=True,
            groups=[
                iam.ComplexValue(display="users", ref="Groups/2", value="2"),
                iam.ComplexValue(display="not_admins", ref="Groups/3", value="3"),
            ],
        ),
        iam.User(
            user_name="not_admin_2",
            active=True,
            groups=[
                iam.ComplexValue(display="admins", ref="Groups/4", value="4"),
            ],
        ),
    ]
    _setup_accounts(ws, workspace_users=users, groups=groups)

    finder = WorkspaceAdministratorFinder(ws)
    admins = list(finder.find_admin_users())

    expected_admins = {"admin_1", "admin_2"}
    assert len(admins) == len(expected_admins)
    assert set(admin.user_name for admin in admins) == expected_admins


def test_workspace_admin_finder_no_admins(ws) -> None:
    """Verify that the workspace admin finder handles no admins as a normal situation."""
    admins_group = _create_workspace_group("admins", group_id="1")
    _setup_accounts(ws, workspace_users=[], groups=[admins_group])

    finder = WorkspaceAdministratorFinder(ws)
    admins = list(finder.find_admin_users())

    assert not admins


def testa_accounts_admin_finder_active_with_username(ws) -> None:
    """Verify that the account admin finder only reports active users with a user-name."""
    inactive_admin = _create_account_admin("inactive_admin")
    inactive_admin.active = False
    users = [
        _create_account_admin("only_real_admin"),
        inactive_admin,
        _create_account_admin(""),
    ]
    _setup_accounts(ws, account_users=users)

    finder = AccountAdministratorFinder(ws)
    admins = list(finder.find_admin_users())

    assert [admin.user_name for admin in admins] == ["only_real_admin"]


def test_accounts_admin_finder_role(ws) -> None:
    """Verify that the account admin finder only reports users with the 'account_admin' role."""
    users = [
        _create_account_admin("admin_1"),
        iam.User(
            user_name="admin_2",
            active=True,
            roles=[
                iam.ComplexValue(value="account_admin"),
                iam.ComplexValue(value="another_role"),
            ],
        ),
        iam.User(
            user_name="not_admin",
            active=True,
            roles=[
                iam.ComplexValue(value="another_role"),
            ],
        ),
    ]
    _setup_accounts(ws, account_users=users)

    finder = AccountAdministratorFinder(ws)
    admins = list(finder.find_admin_users())

    expected_admins = {"admin_1", "admin_2"}
    assert len(admins) == len(expected_admins)
    assert set(admin.user_name for admin in admins) == expected_admins


def test_accounts_admin_finder_no_admins(ws) -> None:
    """Verify that the workspace admin finder handles no admins as a normal situation."""
    finder = AccountAdministratorFinder(ws)
    admins = list(finder.find_admin_users())

    assert not admins


def test_admin_locator_prefers_workspace_admin_over_account_admin(ws) -> None:
    """Verify that when both workspace and account administrators are configured, the workspace admin is preferred."""
    admins_group = _create_workspace_group("admins", group_id="1")
    assert admins_group.id
    workspace_users = [_create_workspace_admin("bob", admins_group_id=admins_group.id)]
    account_users = [_create_account_admin("jane")]
    _setup_accounts(ws, account_users=account_users, workspace_users=workspace_users, groups=[admins_group])

    locator = AdministratorLocator(ws)
    the_admin = locator.get_workspace_administrator()

    assert the_admin == "bob"
    # Also verify that we didn't attempt to look up account admins.
    ws.api_client.do.assert_not_called()


def test_admin_locator_prefer_first_workspace_admin_alphabetically(ws) -> None:
    """Verify that when multiple workspace administrators can found, the first alphabetically is used."""
    admins_group = _create_workspace_group("admins", group_id="1")
    assert admins_group.id
    workspace_users = [
        _create_workspace_admin("bob", admins_group_id=admins_group.id),
        _create_workspace_admin("andrew", admins_group_id=admins_group.id),
        _create_workspace_admin("jane", admins_group_id=admins_group.id),
    ]
    _setup_accounts(ws, workspace_users=workspace_users, groups=[admins_group])

    locator = AdministratorLocator(ws)
    the_admin = locator.get_workspace_administrator()

    assert the_admin == "andrew"


def test_admin_locator_prefer_first_account_admin_alphabetically(ws) -> None:
    """Verify that when multiple account administrators can found, the first alphabetically preferred is used."""
    account_users = [
        _create_account_admin("bob"),
        _create_account_admin("andrew"),
        _create_account_admin("jane"),
    ]
    _setup_accounts(ws, account_users=account_users)

    locator = AdministratorLocator(ws)
    the_admin = locator.get_workspace_administrator()

    assert the_admin == "andrew"


def test_admin_locator_error_when_no_admin(ws) -> None:
    """Verify that an error is raised when no workspace or account administrators can be found."""
    _setup_accounts(ws)

    locator = AdministratorLocator(ws)
    # No admins.
    workspace_id = ws.get_workspace_id()
    expected_message = f"No active workspace or account administrator can be found for workspace: {workspace_id}"
    with pytest.raises(RuntimeError, match=re.escape(expected_message)):
        _ = locator.get_workspace_administrator()


def test_admin_locator_is_lazy(ws) -> None:
    """Verify that we don't attempt to locate an administrator until it's needed."""
    mock_finder = create_autospec(AdministratorFinder)
    mock_finder.find_admin_users.return_value = (_create_account_admin("bob"),)
    mock_finder_factory = Mock()
    mock_finder_factory.return_value = mock_finder
    locator = AdministratorLocator(ws, finders=[mock_finder_factory])

    mock_finder_factory.assert_not_called()
    mock_finder.assert_not_called()

    _ = locator.get_workspace_administrator()

    mock_finder_factory.assert_called_once_with(ws)
    mock_finder.find_admin_users.assert_called_once()


def test_admin_locator_caches_result(ws) -> None:
    """Verify that locating an administrator only happens once."""
    mock_finder = create_autospec(AdministratorFinder)
    mock_finder.find_admin_users.return_value = (_create_account_admin("bob"),)
    mock_finder_factory = Mock()
    mock_finder_factory.return_value = mock_finder

    locator = AdministratorLocator(ws, finders=[mock_finder_factory])
    _ = locator.get_workspace_administrator()
    _ = locator.get_workspace_administrator()

    mock_finder_factory.assert_called_once_with(ws)
    mock_finder.find_admin_users.assert_called_once()


def test_admin_locator_caches_negative_result(ws) -> None:
    """Verify that locating an administrator only happens once, even if it couldn't locate an admin."""
    mock_finder = create_autospec(AdministratorFinder)
    mock_finder.find_admin_users.return_value = ()
    mock_finder_factory = Mock()
    mock_finder_factory.return_value = mock_finder

    locator = AdministratorLocator(ws, finders=[mock_finder_factory])
    with pytest.raises(RuntimeError):
        _ = locator.get_workspace_administrator()
    with pytest.raises(RuntimeError):
        _ = locator.get_workspace_administrator()

    mock_finder_factory.assert_called_once_with(ws)
    mock_finder.find_admin_users.assert_called_once()


def test_ownership_prefers_record_owner() -> None:
    """Verify that if an owner for the record can be found, that is used."""
    ownership = _OwnershipFixture[str](owner_fn=lambda _: "bob")
    owner = ownership.owner_of("school")

    assert owner == "bob"
    ownership.mock_admin_locator.get_workspace_administrator.assert_not_called()


def test_ownership_admin_user_fallback() -> None:
    """Verify that if no owner for the record can be found, an admin user is returned instead."""
    ownership = _OwnershipFixture[str]()
    ownership.mock_admin_locator.get_workspace_administrator.return_value = "jane"

    owner = ownership.owner_of("school")

    assert owner == "jane"


def test_ownership_no_fallback_admin_user_error() -> None:
    """Verify that if no owner can be determined, an error is raised."""
    ownership = _OwnershipFixture[str]()
    ownership.mock_admin_locator.get_workspace_administrator.side_effect = RuntimeError("Mocked admin lookup failure.")

    with pytest.raises(RuntimeError, match="Mocked admin lookup failure."):
        _ = ownership.owner_of("school")


def test_workspace_path_ownership_for_invalid_path() -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "Admin"
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.side_effect = InvalidParameterValue("Invalid path")
    ownership = WorkspacePathOwnership(administrator_locator, ws)

    owner = ownership.owner_of(WorkspacePath(ws, "invalid/path/misses/leading/backslash"))

    assert owner == "Admin"
    administrator_locator.get_workspace_administrator.assert_called_once()
    ws.permissions.get.assert_not_called()


def test_workspace_path_ownership_warns_about_unsupported_object_type(caplog) -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "Admin"
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(object_id=1, object_type=ObjectType.REPO)
    ownership = WorkspacePathOwnership(administrator_locator, ws)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.framework.owners"):
        owner = ownership.owner_of(WorkspacePath(ws, "/Workspace/Repose/repo"))

    assert owner == "Admin"
    assert "Unsupported object type: REPO" in caplog.messages
    administrator_locator.get_workspace_administrator.assert_called_once()
    ws.permissions.get.assert_not_called()


def test_workspace_path_ownership_for_directory() -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(object_id=1, object_type=ObjectType.DIRECTORY)
    can_manage_permission = iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE)
    access_control_list = [iam.AccessControlResponse(all_permissions=[can_manage_permission], user_name="cor")]
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=access_control_list)
    ownership = WorkspacePathOwnership(administrator_locator, ws)

    owner = ownership.owner_of(WorkspacePath(ws, "/some/directory"))

    assert owner == "cor"
    administrator_locator.get_workspace_administrator.assert_not_called()
    ws.permissions.get.assert_called_with("directories", "1")
