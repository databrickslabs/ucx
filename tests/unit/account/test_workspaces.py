import base64
import json

import pytest
from databricks.sdk.core import Config
from databricks.sdk.oauth import Token
from databricks.sdk.service.provisioning import PricingTier, Workspace

from databricks.labs.ucx.account.workspaces import AzureWorkspaceLister, Workspaces
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


def test_subscriptions_name_to_id(arm_requests):
    arm_requests(
        {
            "/subscriptions": {
                "value": [
                    {"displayName": "first", "subscriptionId": "001", "tenantId": "xxx"},
                    {"displayName": "second", "subscriptionId": "002", "tenantId": "def_from_token"},
                    {"displayName": "third", "subscriptionId": "003", "tenantId": "def_from_token"},
                ]
            }
        }
    )
    cfg = Config(host="https://accounts.azuredatabricks.net", auth_type="azure-cli")

    awl = AzureWorkspaceLister(cfg)
    subs = awl.subscriptions_name_to_id()

    assert {"second": "002", "third": "003"} == subs


def test_list_azure_workspaces(arm_requests):
    arm_requests(
        {
            "/subscriptions": {
                "value": [
                    {"displayName": "first", "subscriptionId": "001", "tenantId": "xxx"},
                    {"displayName": "second", "subscriptionId": "002", "tenantId": "def_from_token"},
                    {"displayName": "third", "subscriptionId": "003", "tenantId": "def_from_token"},
                ]
            },
            "/subscriptions/002/providers/Microsoft.Databricks/workspaces": {
                "value": [
                    {
                        "id": ".../resourceGroups/first-rg/...",
                        "name": "first-workspace",
                        "location": "eastus",
                        "sku": {"name": "premium"},
                        "properties": {
                            "provisioningState": "Succeeded",
                            "workspaceUrl": "adb-123.10.azuredatabricks.net",
                            "workspaceId": "123",
                        },
                    },
                    {
                        "id": ".../resourceGroups/first-rg/...",
                        "name": "second-workspace",
                        "location": "eastus",
                        "sku": {"name": "premium"},
                        "properties": {
                            "provisioningState": "Succeeded",
                            "workspaceUrl": "adb-123.10.azuredatabricks.net",
                            "workspaceId": "123",
                        },
                    },
                ]
            },
        }
    )

    wrksp = Workspaces(
        AccountConfig(
            connect=ConnectConfig(host="https://accounts.azuredatabricks.net"),
            include_workspace_names=["first-workspace"],
            include_azure_subscription_names=["second"],
        )
    )

    all_workspaces = list(wrksp.configured_workspaces())

    assert [
        Workspace(
            cloud="azure",
            location="eastus",
            workspace_id=123,
            pricing_tier=PricingTier.PREMIUM,
            workspace_name="first-workspace",
            deployment_name="adb-123.10",
            workspace_status_message="Succeeded",
            custom_tags={"AzureResourceGroup": "first-rg", "AzureSubscription": "second", "AzureSubscriptionID": "002"},
        )
    ] == all_workspaces


def test_client_for_workspace():
    wrksp = Workspaces(
        AccountConfig(
            connect=ConnectConfig(
                host="https://accounts.azuredatabricks.net",
                azure_tenant_id="abc",
                azure_client_id="bcd",
                azure_client_secret="def",
            )
        )
    )
    specified_workspace_client = wrksp.client_for(Workspace(cloud="azure", deployment_name="adb-123.10"))
    assert "azure-client-secret" == specified_workspace_client.config.auth_type
    assert "https://adb-123.10.azuredatabricks.net" == specified_workspace_client.config.host
