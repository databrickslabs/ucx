import datetime as dt
import os
import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.azure.resources import AccessConnector, AccessConnectorClient, AzureAPIClient


@pytest.fixture
def azure_management_api_client(ws: WorkspaceClient) -> AzureAPIClient:
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    return azure_mgmt_client


@pytest.fixture
def access_connector_client(azure_management_api_client: AzureAPIClient) -> AccessConnectorClient:
    access_connector_client = AccessConnectorClient(azure_management_api_client)
    return access_connector_client


@pytest.mark.skip(reason="Can not test CRUD operations on Azure resources in CI")
def test_access_connector_client_create_delete(ws, access_connector_client: AccessConnectorClient):
    subscription_id = os.getenv("TEST_SUBSCRIPTION_ID", "unknown-subscription-id")
    resource_group = os.getenv("TEST_RESOURCE_GROUP", "unknown-resource-group")
    access_connector_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers"
        "/Microsoft.Databricks/accessConnectors/test"
    )

    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    access_connector = AccessConnector(
        id=access_connector_id,
        location="westeurope",
        tags={"RemoveAfter": str(tomorrow.date())},
    )

    assert access_connector not in access_connector_client.list(subscription_id)
    access_connector_client.create(access_connector)
    assert access_connector in access_connector_client.list(subscription_id)
    access_connector_client.delete(access_connector)
    assert access_connector not in access_connector_client.list(subscription_id)
