import io
import json
from unittest.mock import create_autospec, patch

from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.config import AccountConfig, ConnectConfig, WorkspaceConfig
from databricks.labs.ucx.installer import InstallationManager, InstallationUCX


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
        InstallationUCX(
            config=WorkspaceConfig(inventory_database="ucx"), username=User(display_name="foo"), path="/Users/foo"
        )
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
