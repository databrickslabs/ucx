import io
import json
from unittest.mock import create_autospec, patch, MagicMock

from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User, Group, ComplexValue
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.config import AccountConfig, ConnectConfig, WorkspaceConfig
from databricks.labs.ucx.installer import Installation, InstallationManager


def test_sync_workspace_info(mocker):
    account_config = AccountConfig(
        connect=ConnectConfig(host="https://accounts.cloud.databricks.com", account_id="123", token="abc")
    )
    # TODO: https://github.com/databricks/databricks-sdk-py/pull/480
    acc_client = mocker.patch("databricks.sdk.AccountClient.__init__")
    acc_client.config = account_config.to_databricks_config()

    account_config.to_account_client = lambda: acc_client
    # test for workspace filtering
    account_config.include_workspace_names = ["foo"]

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    def workspace_client(host, product, **kwargs) -> WorkspaceClient:
        assert host == "https://abc.cloud.databricks.com"
        assert product == "ucx"
        return ws

    im = create_autospec(InstallationManager)
    im.user_installations.return_value = [
        Installation(config=WorkspaceConfig(inventory_database="ucx"), user=User(display_name="foo"), path="/Users/foo")
    ]

    account_workspaces = AccountWorkspaces(account_config, workspace_client, lambda _: im)
    account_workspaces.sync_workspace_info()

    ws.workspace.upload.assert_called_with(
        "/Users/foo/workspaces.json",
        b'[\n  {\n    "deployment_name": "abc",\n    "workspace_id": 123,\n    "worksp'
        b'ace_name": "foo",\n    "workspace_status_message": "Running"\n  }\n]',
        overwrite=True,
        format=ImportFormat.AUTO,
    )


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

        wir = WorkspaceInfo(ws, "/foo")
        assert "some" == wir.current()


def test_manual_workspace_info(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.config.host = "localhost"

    im = create_autospec(InstallationManager)
    im.user_installations.return_value = [
        Installation(config=WorkspaceConfig(inventory_database="ucx"), user=User(display_name="foo"), path="/Users/foo")
    ]

    ws.config.host = "localhost"
    ws.config.user_agent = "ucx"
    ws.config.authenticate.return_value = {"Foo": "bar"}
    ws.workspace.download.return_value = json.dumps([{"workspace_id": 123, "workspace_name": "some"}])
    with patch("requests.get") as requests_get:
        response = mocker.Mock()
        response.headers = {"x-databricks-org-id": "123"}
        requests_get.return_value = response

        wir = WorkspaceInfo(ws, "/foo", lambda _: im)
        prompts = MockPrompts({r"Workspace name for 123": "some-name", r"Next workspace id": "stop"})

        wir.manual_workspace_info(prompts)

    ws.workspace.upload.assert_called_with(
        "/Users/foo/workspaces.json",
        b'[\n  {\n    "workspace_id": 123,\n    "workspace_name": "some-name"\n  }\n]',
        overwrite=True,
        format=ImportFormat.AUTO,
    )

def test_create_acc_groups_should_create_acc_group_if_no_group_found(mocker):
    account_config = AccountConfig(
        connect=ConnectConfig(host="https://accounts.cloud.databricks.com", account_id="123", token="abc")
    )
    # TODO: https://github.com/databricks/databricks-sdk-py/pull/480
    acc_client = mocker.patch("databricks.sdk.AccountClient.__init__")
    acc_client.config = account_config.to_databricks_config()

    account_config.to_account_client = lambda: acc_client
    account_config.include_workspace_names = ["foo"]

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    mock1 = MagicMock()

    def workspace_client(**kwargs) -> WorkspaceClient:
        return mock1

    group = Group(
        id= "12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )

    mock1.groups.list.return_value = [group]
    mock1.groups.get.return_value = group

    account_workspaces = AccountWorkspaces(account_config, workspace_client)
    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_called_with(
        display_name='de',
        members=[
            ComplexValue(display='test-user-1', primary=None, type=None, value='20'),
            ComplexValue(display='test-user-2', primary=None, type=None, value='21')
        ]
    )


def test_create_acc_groups_should_filter_groups_in_other_workspaces(mocker):
    account_config = AccountConfig(
        connect=ConnectConfig(host="https://accounts.cloud.databricks.com", account_id="123", token="abc")
    )
    # TODO: https://github.com/databricks/databricks-sdk-py/pull/480
    acc_client = mocker.patch("databricks.sdk.AccountClient.__init__")
    acc_client.config = account_config.to_databricks_config()

    account_config.to_account_client = lambda: acc_client
    account_config.include_workspace_names = ["foo", "bar"]

    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"),
        Workspace(workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"),
    ]

    mock1 = MagicMock()
    mock2 = MagicMock()

    def workspace_client(host, product, **kwargs) -> WorkspaceClient:
        if host == "https://abc.cloud.databricks.com":
            return mock1
        else:
            return mock2

    im = create_autospec(InstallationManager)
    im.user_installations.return_value = [
        Installation(config=WorkspaceConfig(inventory_database="ucx"), user=User(display_name="foo"), path="/Users/foo")
    ]

    group = Group(
        id= "12",
        display_name="de",
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
    )

    mock1.groups.list.return_value = [group]
    mock1.groups.get.return_value = group

    mock2.groups.list.return_value = [group]
    mock2.groups.get.return_value = group

    account_workspaces = AccountWorkspaces(account_config, workspace_client, lambda _: im)
    account_workspaces.create_account_level_groups()

    acc_client.groups.create.assert_called_once_with(
        display_name='de',
        members=[
            ComplexValue(display='test-user-1', primary=None, type=None, value='20'),
            ComplexValue(display='test-user-2', primary=None, type=None, value='21')
        ]
    )



