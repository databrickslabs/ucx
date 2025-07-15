import logging
import io
import json
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceConflict
from databricks.sdk.service import iam
from databricks.sdk.service.iam import ComplexValue, Group, ResourceMeta, User
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.ucx.account.workspaces import AccountWorkspaces, WorkspaceInfo


def test_sync_workspace_info(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws = create_autospec(WorkspaceClient)
    acc_client.get_workspace_client.return_value = ws

    account_workspaces = AccountWorkspaces(acc_client)
    account_workspaces.sync_workspace_info()

    ws.workspace.upload.assert_called()


def test_current_workspace_name():
    ws = create_autospec(WorkspaceClient)
    ws.config.host = "localhost"
    ws.config.user_agent = "ucx"
    ws.config.authenticate.return_value = {"Foo": "bar"}
    ws.workspace.download.return_value = io.StringIO(json.dumps([{"workspace_id": 123, "workspace_name": "some"}]))
    ws.get_workspace_id.return_value = 123
    installation = Installation(ws, 'ucx')
    wir = WorkspaceInfo(installation, ws)
    assert wir.current() == "some"


def test_manual_workspace_info():
    ws = create_autospec(WorkspaceClient)
    ws.config.host = "localhost"
    ws.users.list.return_value = [User(user_name="foo")]
    ws.config.host = "localhost"
    ws.config.user_agent = "ucx"
    ws.config.authenticate.return_value = {"Foo": "bar"}
    ws.workspace.download.return_value = json.dumps([{"workspace_id": 123, "workspace_name": "some"}])
    ws.get_workspace_id.return_value = 123
    installation = MockInstallation()
    wir = WorkspaceInfo(installation, ws)
    prompts = MockPrompts({r"Workspace name for 123": "some-name", r"Next workspace id": "stop"})
    wir.manual_workspace_info(prompts)
    ws.workspace.upload.assert_called()


def test_create_acc_groups_should_create_acc_group_if_no_group_found_in_account(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)

    group = Group(
        id="12",
        display_name="de",
        members=[
            ComplexValue(display="test-user-1", value="20", ref="Users/20"),
            ComplexValue(display="test-user-2", value="21", ref="Users/21"),
        ],
        meta=ResourceMeta("WorkspaceGroup"),
    )
    group_2 = Group(
        display_name="no_id",
        members=[
            ComplexValue(display="test-user-1", value="20", ref="Users/20"),
            ComplexValue(display="test-user-2", value="21", ref="Users/21"),
        ],
    )

    ws.groups.list.return_value = [group, group_2]
    ws.groups.get.return_value = group
    acc_client.get_workspace_client.return_value = ws
    acc_client.groups.create.return_value = group

    account_workspaces = AccountWorkspaces(acc_client, [123, 46])
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_called_with(
        display_name="de",
    )
    acc_client.groups.patch.assert_called_with(
        "12",
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path='members',
                value=[
                    {
                        'display': 'test-user-1',
                        'value': '20',
                        '$ref': 'Users/20',
                    },
                    {
                        'display': 'test-user-2',
                        'value': '21',
                        '$ref': 'Users/21',
                    },
                ],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_create_acc_groups_should_throw_exception(acc_client):
    acc_client.workspaces.list.return_value = []

    ws = create_autospec(WorkspaceClient)
    group = Group(id="12", display_name="test_account", meta=ResourceMeta("Account"))

    acc_client.get_workspace_client.return_value = ws
    acc_client.groups.create.return_value = group

    account_workspaces = AccountWorkspaces(acc_client, [123])
    with pytest.raises(ValueError):
        account_workspaces.create_account_level_groups(MockPrompts({}))

    ws.groups.list.assert_not_called()


def test_create_acc_groups_should_filter_system_groups(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)
    group = Group(
        id="12",
        display_name="admins",
        members=[],
    )

    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group
    acc_client.get_workspace_client.return_value = ws
    acc_client.groups.create.return_value = group

    account_workspaces = AccountWorkspaces(acc_client, [123])
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_not_called()


def test_create_acc_groups_should_filter_account_groups_in_workspace(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)
    group = Group(id="12", display_name="test_account", meta=ResourceMeta("Account"))

    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group
    acc_client.get_workspace_client.return_value = ws
    acc_client.groups.create.return_value = group

    account_workspaces = AccountWorkspaces(acc_client)
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_not_called()


def test_create_acc_groups_should_create_acc_group_with_appropriate_members(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)
    account_workspaces = AccountWorkspaces(acc_client, [123])

    group = Group(
        id="12",
        display_name="de",
        members=[
            ComplexValue(display="test-user-1", value="1", ref='Users/1'),
            ComplexValue(display="test-user-2", value="2", ref='Users/2'),
            ComplexValue(display="test-user-3", value="3", ref='Users/3'),
            ComplexValue(display="test-user-4", value="4", ref='Users/4'),
            ComplexValue(display="test-user-5", value="5", ref='Users/5'),
            ComplexValue(display="test-user-6", value="6", ref='Users/6'),
            ComplexValue(display="test-user-7", value="7", ref='Users/7'),
            ComplexValue(display="test-user-8", value="8", ref='Users/8'),
            ComplexValue(display="test-user-9", value="9", ref='Users/9'),
            ComplexValue(display="test-user-10", value="10", ref='Users/10'),
            ComplexValue(display="test-user-11", value="11", ref='Users/11'),
            ComplexValue(display="test-user-12", value="12", ref='Users/12'),
            ComplexValue(display="test-user-13", value="13", ref='Users/13'),
            ComplexValue(display="test-user-14", value="14", ref='Users/14'),
            ComplexValue(display="test-user-15", value="15", ref='Users/15'),
            ComplexValue(display="test-user-16", value="16", ref='Users/16'),
            ComplexValue(display="test-user-17", value="17", ref='Users/17'),
            ComplexValue(display="test-user-18", value="18", ref='Users/18'),
            ComplexValue(display="test-user-19", value="19", ref='Users/19'),
            ComplexValue(display="test-user-20", value="20", ref='Users/20'),
            ComplexValue(display="test-user-21", value="21", ref='Users/21'),
            ComplexValue(display="test-user-22", value="22", ref='Users/22'),
            ComplexValue(display="test-user-23", value="23", ref='Users/23'),
            ComplexValue(display="test-user-24", value="24", ref='Users/24'),
            ComplexValue(display="test-user-25", value="25", ref='Users/25'),
        ],
    )

    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group
    acc_client.get_workspace_client.return_value = ws
    acc_client.groups.create.return_value = group

    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_called_with(
        display_name="de",
    )
    acc_client.groups.patch.assert_any_call(
        "12",
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path='members',
                value=[
                    {'display': 'test-user-1', 'value': '1', '$ref': 'Users/1'},
                    {'display': 'test-user-2', 'value': '2', '$ref': 'Users/2'},
                    {'display': 'test-user-3', 'value': '3', '$ref': 'Users/3'},
                    {'display': 'test-user-4', 'value': '4', '$ref': 'Users/4'},
                    {'display': 'test-user-5', 'value': '5', '$ref': 'Users/5'},
                    {'display': 'test-user-6', 'value': '6', '$ref': 'Users/6'},
                    {'display': 'test-user-7', 'value': '7', '$ref': 'Users/7'},
                    {'display': 'test-user-8', 'value': '8', '$ref': 'Users/8'},
                    {'display': 'test-user-9', 'value': '9', '$ref': 'Users/9'},
                    {'display': 'test-user-10', 'value': '10', '$ref': 'Users/10'},
                    {'display': 'test-user-11', 'value': '11', '$ref': 'Users/11'},
                    {'display': 'test-user-12', 'value': '12', '$ref': 'Users/12'},
                    {'display': 'test-user-13', 'value': '13', '$ref': 'Users/13'},
                    {'display': 'test-user-14', 'value': '14', '$ref': 'Users/14'},
                    {'display': 'test-user-15', 'value': '15', '$ref': 'Users/15'},
                    {'display': 'test-user-16', 'value': '16', '$ref': 'Users/16'},
                    {'display': 'test-user-17', 'value': '17', '$ref': 'Users/17'},
                    {'display': 'test-user-18', 'value': '18', '$ref': 'Users/18'},
                    {'display': 'test-user-19', 'value': '19', '$ref': 'Users/19'},
                    {'display': 'test-user-20', 'value': '20', '$ref': 'Users/20'},
                ],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )
    acc_client.groups.patch.assert_any_call(
        "12",
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path='members',
                value=[
                    {'display': 'test-user-21', 'value': '21', '$ref': 'Users/21'},
                    {'display': 'test-user-22', 'value': '22', '$ref': 'Users/22'},
                    {'display': 'test-user-23', 'value': '23', '$ref': 'Users/23'},
                    {'display': 'test-user-24', 'value': '24', '$ref': 'Users/24'},
                    {'display': 'test-user-25', 'value': '25', '$ref': 'Users/25'},
                ],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_create_acc_groups_should_not_create_group_if_exists_in_account(acc_client):
    group = Group(
        id="12",
        display_name="de",
        members=[
            ComplexValue(display="test-user-1", value="20", ref="Users/20"),
            ComplexValue(display="test-user-2", value="21", ref="Users/21"),
        ],
    )
    group_2 = Group(display_name="de_invalid")
    acc_client.groups.list.return_value = [group, group_2]
    acc_client.groups.get.return_value = group
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)
    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group
    acc_client.get_workspace_client.return_value = ws
    account_workspaces = AccountWorkspaces(acc_client, [123])
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_not_called()


def test_create_acc_groups_should_create_groups_accross_workspaces(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws1 = create_autospec(WorkspaceClient)
    ws2 = create_autospec(WorkspaceClient)

    def get_workspace_client(workspace) -> WorkspaceClient:
        if workspace.workspace_id == 123:
            return ws1
        return ws2

    group = Group(id="12", display_name="de", members=[])
    group2 = Group(id="12", display_name="security_grp", members=[])

    ws1.groups.list.return_value = [group]
    ws1.groups.get.return_value = group

    ws2.groups.list.return_value = [group2]
    ws2.groups.get.return_value = group2

    acc_client.get_workspace_client.side_effect = get_workspace_client

    account_workspaces = AccountWorkspaces(acc_client, [123, 456])
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_any_call(display_name="de")
    acc_client.groups.create.assert_any_call(display_name="security_grp")


def test_create_acc_groups_should_filter_groups_accross_workspaces(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws1 = create_autospec(WorkspaceClient)
    ws2 = create_autospec(WorkspaceClient)

    def get_workspace_client(workspace) -> WorkspaceClient:
        if workspace.workspace_id == 123:
            return ws1
        return ws2

    group = Group(
        id="12",
        display_name="de",
        members=[
            ComplexValue(display="test-user-1", value="20", ref="Users/20"),
            ComplexValue(display="test-user-2", value="21", ref="Users/21"),
        ],
    )

    ws1.groups.list.return_value = [group]
    ws1.groups.get.return_value = group

    ws2.groups.list.return_value = [group]
    ws2.groups.get.return_value = group
    acc_client.groups.create.return_value = group
    acc_client.get_workspace_client.side_effect = get_workspace_client

    account_workspaces = AccountWorkspaces(acc_client, [123, 456])
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_called_once_with(display_name="de")
    acc_client.groups.patch.assert_called_once_with(
        "12",
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path='members',
                value=[
                    {'display': 'test-user-1', 'value': '20', '$ref': 'Users/20'},
                    {'display': 'test-user-2', 'value': '21', '$ref': 'Users/21'},
                ],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_create_acc_groups_should_create_acc_group_if_exist_in_other_workspaces_but_not_same_members(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="ws1", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="ws2", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws1 = create_autospec(WorkspaceClient)
    ws2 = create_autospec(WorkspaceClient)

    def get_workspace_client(workspace) -> WorkspaceClient:
        if workspace.workspace_id == 123:
            return ws1
        return ws2

    group = Group(
        id="12",
        display_name="de",
        members=[
            ComplexValue(display="test-user-1", value="20", ref="Users/20"),
            ComplexValue(display="test-user-2", value="21", ref="Users/21"),
        ],
    )
    group_2 = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20", ref="Users/20")],
    )

    ws1.groups.list.return_value = [group]
    ws1.groups.get.return_value = group

    ws2.groups.list.return_value = [group_2]
    ws2.groups.get.return_value = group_2
    acc_client.get_workspace_client.side_effect = get_workspace_client

    account_workspaces = AccountWorkspaces(acc_client, [123, 456])
    account_workspaces.create_account_level_groups(
        MockPrompts(
            {
                r'Group de does not have the same amount of members in workspace ': 'yes',
            }
        )
    )

    acc_client.groups.create.assert_any_call(display_name="de")
    acc_client.groups.create.assert_any_call(display_name="ws2_de")


def test_acc_ws_get_should_not_throw(acc_client):
    group = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )
    group_2 = Group(display_name="de_invalid")
    acc_client.groups.list.return_value = [group, group_2]
    acc_client.groups.get.side_effect = NotFound
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)

    ws.groups.list.return_value = [group]
    ws.groups.get.side_effect = NotFound
    acc_client.get_workspace_client.return_value = ws
    account_workspaces = AccountWorkspaces(acc_client, [123])
    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_not_called()


def test_create_acc_groups_should_not_throw_if_acc_grp_exists(acc_client):
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)
    account_workspaces = AccountWorkspaces(acc_client, [123])

    group = Group(id="12", display_name="de", members=[ComplexValue(display="test-user-1", value="1", ref="Users/1")])

    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group
    acc_client.get_workspace_client.return_value = ws
    acc_client.groups.create.side_effect = ResourceConflict

    account_workspaces.create_account_level_groups(MockPrompts({}))

    acc_client.groups.create.assert_called_with(display_name="de")
    acc_client.groups.patch.assert_not_called()


def test_get_accessible_workspaces():
    acc = create_autospec(AccountClient)
    acc.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
        Workspace(workspace_name="bar", workspace_id=789, workspace_status_message="Running", deployment_name="def"),
        Workspace(workspace_name="bar", workspace_id=000, workspace_status_message="Running", deployment_name="def"),
    ]
    acc.config.is_azure = True
    acc.config.auth_type = "databricks-cli"

    # admin in workspace 1
    ws1 = create_autospec(WorkspaceClient)
    ws1.current_user.me.return_value = iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    # not an admin in workspace 2
    ws2 = create_autospec(WorkspaceClient)
    ws2.current_user.me.return_value = iam.User(user_name="me@example.com")
    # not an admin in workspace 3
    ws3 = create_autospec(WorkspaceClient)
    ws3.current_user.me.return_value = iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="users")])

    def get_workspace_client(workspace) -> WorkspaceClient:
        match workspace.workspace_id:
            case 123:
                return ws1
            case 456:
                return ws2
            case 789:
                return ws3
            case _:
                raise ValueError("unexpected workspace id")

    acc.get_workspace_client.side_effect = get_workspace_client

    account_workspaces = AccountWorkspaces(acc)
    assert len(account_workspaces.get_accessible_workspaces()) == 1
    ws1.current_user.me.assert_called_once()
    ws2.current_user.me.assert_called_once()
    # get_workspace_client should be called once for 123, 456, 789, then twice for workspace 000
    assert acc.get_workspace_client.call_count == 5

    acc.config.is_azure = False
    acc.config.auth_type = "databricks-cli"
    account_workspaces = AccountWorkspaces(acc)
    assert len(account_workspaces.get_accessible_workspaces()) == 1


def test_account_workspaces_can_administer_when_user_in_admins_group() -> None:
    acc = create_autospec(AccountClient)
    ws = create_autospec(WorkspaceClient)
    acc.get_workspace_client.return_value = ws
    ws.current_user.me.return_value = User(user_name="test", groups=[ComplexValue(display="admins")])
    account_workspaces = AccountWorkspaces(acc)
    workspace = Workspace(deployment_name="test")

    assert account_workspaces.can_administer(workspace)


@pytest.mark.parametrize("groups", [[ComplexValue(display="not-admins")], None])
def test_account_workspaces_cannot_administer_when_user_not_in_admins_group(caplog, groups) -> None:
    acc = create_autospec(AccountClient)
    ws = create_autospec(WorkspaceClient)
    acc.get_workspace_client.return_value = ws
    ws.current_user.me.return_value = User(user_name="test", groups=groups)
    account_workspaces = AccountWorkspaces(acc)
    workspace = Workspace(deployment_name="test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.workspaces"):
        can_administer = account_workspaces.can_administer(workspace)
    assert not can_administer
    assert "User 'test' is not a workspace admin: test" in caplog.messages


def test_account_workspaces_can_administer_handles_not_found_error_for_get_workspace_client(caplog) -> None:
    acc = create_autospec(AccountClient)
    acc.get_workspace_client.side_effect = NotFound
    account_workspaces = AccountWorkspaces(acc)
    workspace = Workspace(deployment_name="test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.workspaces"):
        can_administer = account_workspaces.can_administer(workspace)
    assert not can_administer
    assert "User cannot access workspace: test" in caplog.messages


def test_account_workspaces_can_administer_handles_permission_denied_error_for_current_user(caplog) -> None:
    acc = create_autospec(AccountClient)
    ws = create_autospec(WorkspaceClient)
    acc.get_workspace_client.return_value = ws
    ws.current_user.me.side_effect = PermissionDenied(
        "This API is disabled for users without the databricks-sql-access or workspace-access entitlements"
    )
    account_workspaces = AccountWorkspaces(acc)
    workspace = Workspace(deployment_name="test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.workspaces"):
        can_administer = account_workspaces.can_administer(workspace)
    assert not can_administer
    assert "User cannot access workspace: test" in caplog.messages


def test_account_workspaces_workspace_clients_handles_permission_denied(caplog) -> None:
    acc = create_autospec(AccountClient)
    acc.get_workspace_client.side_effect = PermissionDenied
    account_workspaces = AccountWorkspaces(acc)

    try:
        with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.workspaces"):
            clients = account_workspaces.workspace_clients([Workspace(deployment_name="test")])
    except PermissionDenied:
        assert False, "Workspace clients does not handle `PermissionDenied`"
    else:
        assert len(clients) == 0
        assert "Cannot get a workspace client for: test" in caplog.messages
