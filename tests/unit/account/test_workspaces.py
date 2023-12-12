import base64
import json

import pytest
from databricks.sdk.oauth import Token
from databricks.sdk.service.iam import User
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.ucx.account.workspaces import Workspaces
from databricks.labs.ucx.config import AccountConfig, ConnectConfig


@pytest.fixture
def arm_requests(mocker):
    claims = {"tid": "def_from_token"}

    token = mocker.patch("databricks.sdk.oauth.Refreshable.token")
    jwt_claims = json.dumps(claims).encode("utf8")
    almost_jwt = base64.b64encode(jwt_claims).decode("utf8").rstrip("=")
    token.return_value = Token(token_type="Bearer", access_token=f"ignore.{almost_jwt}.ignore")

    def inner(path_to_response: dict[str, dict]):
        def mock_response(endpoint, **kwargs):
            endpoint = endpoint.replace("https://management.azure.com/", "")
            if endpoint not in path_to_response:
                msg = f"no mock for {endpoint}"
                raise KeyError(msg)
            response = mocker.Mock()
            response.json.return_value = path_to_response[endpoint]
            return response

        mocker.patch("requests.get", side_effect=mock_response)

    return inner


@pytest.fixture()
def workspace_mock(mocker):
    acc_cfg = AccountConfig()
    acc_client = mocker.patch("databricks.sdk.AccountClient.__init__")
    acc_cfg.to_databricks_config = lambda: acc_client
    acc_client.config = mocker.Mock()
    acc_client.config.as_dict = lambda: {}

    acc_cfg.to_account_client = lambda: acc_client
    acc_cfg.include_workspace_names = ["foo", "bar"]
    mock_workspace1 = Workspace(
        workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc"
    )
    mock_workspace2 = Workspace(
        workspace_name="bar", workspace_id=456, workspace_status_message="Running", deployment_name="def"
    )

    mock_user1 = User(user_name="jack")
    acc_client.workspaces.users.list.return_value = [mock_user1]
    acc_client.workspaces.list.return_value = [mock_workspace1, mock_workspace2]
    return Workspaces(acc_cfg)


def test_client_for_workspace():
    wrksp = Workspaces(
        AccountConfig(
            connect=ConnectConfig(
                host="https://accounts.azuredatabricks.net",
                azure_tenant_id="abc.com",
                azure_client_id="bcd",
                azure_client_secret="def",
            )
        )
    )
    specified_workspace_client = wrksp.client_for(Workspace(cloud="azure", deployment_name="adb-123.10"))
    assert "azure-client-secret" == specified_workspace_client.config.auth_type
    assert "https://adb-123.10.azuredatabricks.net" == specified_workspace_client.config.host


def test_workspace_clients(workspace_mock):
    ws_clients = workspace_mock.workspace_clients()
    assert len(ws_clients) == 2
    assert ws_clients[0].config.auth_type == "azure-cli"
    assert ws_clients[0].config.host == "https://abc.azuredatabricks.net"


def test_configured_workspaces(workspace_mock):
    ws_clients = []
    for ws in workspace_mock.configured_workspaces():
        ws_clients.append(workspace_mock.client_for(ws))

    # test for number of workspaces returned
    assert len(ws_clients) == 2

    # test for cloud and deployment name
    assert ws_clients[1].config.auth_type == "azure-cli"
    assert ws_clients[1].config.host == "https://def.azuredatabricks.net"
