import io
import json
from unittest.mock import create_autospec, patch

from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service import iam
from databricks.sdk.service.iam import ComplexValue, Group, User
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo


def test_sync_workspace_info(mocker):
    acc_client = create_autospec(AccountClient)
    acc_client.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws = create_autospec(WorkspaceClient)

    def workspace_client(host, product, **kwargs) -> WorkspaceClient:
        assert host in ("https://abc.cloud.databricks.com", "https://def.cloud.databricks.com")
        assert product == "ucx"
        return ws

    account_workspaces = AccountWorkspaces(acc_client, workspace_client)
    account_workspaces.sync_workspace_info()

    ws.workspace.upload.assert_called()


def test_current_workspace_name(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.config.host = "localhost"
    ws.config.user_agent = "ucx"
    ws.config.authenticate.return_value = {"Foo": "bar"}
    ws.workspace.download.return_value = io.StringIO(json.dumps([{"workspace_id": 123, "workspace_name": "some"}]))
    with patch("requests.get") as requests_get:
        response = mocker.Mock()
        response.headers = {"x-databricks-org-id": "123"}
        requests_get.return_value = response
        installation = Installation(ws, 'ucx')
        wir = WorkspaceInfo(installation, ws)
        assert "some" == wir.current()


def test_manual_workspace_info(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.config.host = "localhost"
    ws.users.list.return_value = [User(user_name="foo")]
    ws.config.host = "localhost"
    ws.config.user_agent = "ucx"
    ws.config.authenticate.return_value = {"Foo": "bar"}
    ws.workspace.download.return_value = json.dumps([{"workspace_id": 123, "workspace_name": "some"}])
    installation = MockInstallation()
    with patch("requests.get") as requests_get:
        response = mocker.Mock()
        response.headers = {"x-databricks-org-id": "123"}
        requests_get.return_value = response
        wir = WorkspaceInfo(installation, ws)
        prompts = MockPrompts({r"Workspace name for 123": "some-name", r"Next workspace id": "stop"})

        wir.manual_workspace_info(prompts)

    ws.workspace.upload.assert_called()


def test_create_acc_groups_should_create_acc_group_if_no_group_found_in_account(mocker):
    acc_client = create_autospec(AccountClient)
    acc_client.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)

    def workspace_client(**kwargs) -> WorkspaceClient:
        return ws

    account_workspaces = AccountWorkspaces(acc_client, workspace_client)

    group = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )

    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group
    acc_client.groups.create.return_value = group

    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_called_with(
        display_name="de",
    )
    acc_client.groups.patch.assert_called_with(
        "12",
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path='members',
                value=[{'display': 'test-user-1', 'value': '20'}, {'display': 'test-user-2', 'value': '21'}],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_create_acc_groups_should_not_create_group_if_exists_in_account(mocker):
    acc_client = create_autospec(AccountClient)
    acc_client.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")

    group = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )
    acc_client.groups.list.return_value = [group]
    acc_client.groups.get.return_value = group
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)

    def workspace_client(**kwargs) -> WorkspaceClient:
        return ws

    ws.groups.list.return_value = [group]
    ws.groups.get.return_value = group

    account_workspaces = AccountWorkspaces(acc_client, workspace_client)
    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_not_called()


def test_create_acc_groups_should_create_groups_accross_workspaces(mocker):
    acc_client = create_autospec(AccountClient)
    acc_client.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws1 = create_autospec(WorkspaceClient)
    ws2 = create_autospec(WorkspaceClient)

    def workspace_client(host, product, **kwargs) -> WorkspaceClient:
        if host == "https://abc.cloud.databricks.com":
            return ws1
        else:
            return ws2

    group = Group(id="12", display_name="de")
    group2 = Group(id="12", display_name="security_grp")

    ws1.groups.list.return_value = [group]
    ws1.groups.get.return_value = group

    ws2.groups.list.return_value = [group2]
    ws2.groups.get.return_value = group2

    account_workspaces = AccountWorkspaces(acc_client, workspace_client)
    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_any_call(display_name="de")
    acc_client.groups.create.assert_any_call(display_name="security_grp")


def test_create_acc_groups_should_filter_groups_accross_workspaces(mocker):
    acc_client = create_autospec(AccountClient)
    acc_client.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws1 = create_autospec(WorkspaceClient)
    ws2 = create_autospec(WorkspaceClient)

    def workspace_client(host, product, **kwargs) -> WorkspaceClient:
        if host == "https://abc.cloud.databricks.com":
            return ws1
        else:
            return ws2

    group = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )

    ws1.groups.list.return_value = [group]
    ws1.groups.get.return_value = group

    ws2.groups.list.return_value = [group]
    ws2.groups.get.return_value = group
    acc_client.groups.create.return_value = group

    account_workspaces = AccountWorkspaces(acc_client, workspace_client)
    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_called_once_with(display_name="de")
    acc_client.groups.patch.assert_called_once_with(
        "12",
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path='members',
                value=[{'display': 'test-user-1', 'value': '20'}, {'display': 'test-user-2', 'value': '21'}],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_create_acc_groups_should_create_acc_group_if_exist_in_other_workspaces_but_not_same_members(mocker):
    acc_client = create_autospec(AccountClient)
    acc_client.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="ws1", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="ws2", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws1 = create_autospec(WorkspaceClient)
    ws2 = create_autospec(WorkspaceClient)

    def workspace_client(host, product, **kwargs) -> WorkspaceClient:
        if host == "https://abc.cloud.databricks.com":
            return ws1
        else:
            return ws2

    group = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )
    group_2 = Group(
        id="12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20")],
    )

    ws1.groups.list.return_value = [group]
    ws1.groups.get.return_value = group

    ws2.groups.list.return_value = [group_2]
    ws2.groups.get.return_value = group_2

    account_workspaces = AccountWorkspaces(acc_client, workspace_client)
    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_any_call(display_name="de")
    acc_client.groups.create.assert_any_call(display_name="ws2_de")
