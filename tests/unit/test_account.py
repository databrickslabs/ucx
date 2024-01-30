import io
import json
from unittest.mock import create_autospec, patch

from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.iam import User
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
