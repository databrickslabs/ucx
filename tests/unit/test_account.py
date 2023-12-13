import json
from unittest.mock import create_autospec, patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfoReader
from databricks.labs.ucx.config import AccountConfig, ConnectConfig, WorkspaceConfig
from databricks.labs.ucx.installer import Installation, InstallationManager

# @pytest.fixture
# def arm_requests(mocker):
#     claims = {"tid": "def_from_token"}
#
#     token = mocker.patch("databricks.sdk.oauth.Refreshable.token")
#     jwt_claims = json.dumps(claims).encode("utf8")
#     almost_jwt = base64.b64encode(jwt_claims).decode("utf8").rstrip("=")
#     token.return_value = Token(token_type="Bearer", access_token=f"ignore.{almost_jwt}.ignore")
#
#     def inner(path_to_response: dict[str, dict]):
#         def mock_response(endpoint, **kwargs):
#             endpoint = endpoint.replace("https://management.azure.com/", "")
#             if endpoint not in path_to_response:
#                 msg = f"no mock for {endpoint}"
#                 raise KeyError(msg)
#             response = mocker.Mock()
#             response.json.return_value = path_to_response[endpoint]
#             return response
#
#         mocker.patch("requests.get", side_effect=mock_response)
#
#     return inner


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
    ws.workspace.download.return_value = json.dumps([{"workspace_id": 123, "workspace_name": "some"}])
    with patch("requests.get") as requests_get:
        response = mocker.Mock()
        response.headers = {"x-databricks-org-id": "123"}
        requests_get.return_value = response

        wir = WorkspaceInfoReader(ws, "/foo")
        assert "some" == wir.current()
