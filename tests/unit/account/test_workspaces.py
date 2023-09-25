import base64
import json

from databricks.sdk.core import Config
from databricks.sdk.oauth import Token

from databricks.labs.ucx.account.workspaces import AzureWorkspaceLister


def test_subscriptions_name_to_id(mocker):
    claims = {"tid": "def_from_token"}

    token = mocker.patch("databricks.sdk.oauth.Refreshable.token")
    jwt_claims = json.dumps(claims).encode("utf8")
    almost_jwt = base64.b64encode(jwt_claims).decode("utf8").rstrip("=")
    token.return_value = Token(token_type="Bearer", access_token=f"ignore.{almost_jwt}.ignore")

    response = mocker.Mock()
    response.json.return_value = {
        "value": [
            {"displayName": "first", "subscriptionId": "001", "tenantId": "xxx"},
            {"displayName": "second", "subscriptionId": "002", "tenantId": "def_from_token"},
            {"displayName": "third", "subscriptionId": "003", "tenantId": "def_from_token"},
        ]
    }
    mocker.patch("requests.get", return_value=response)

    cfg = Config(host="https://accounts.azuredatabricks.net", auth_type="azure-cli")

    awl = AzureWorkspaceLister(cfg)
    subs = awl.subscriptions_name_to_id()

    assert {"second": "002", "third": "003"} == subs
