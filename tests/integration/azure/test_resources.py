import pytest
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.azure.resources import AzureAPIClient, AccessConnectorClient, AzureResources


TEST_ACCESS_CONNECTOR_RESPONSE = {"value": [{
    "id": "/subscriptions/test/resourceGroups/rg-test/providers/Microsoft.Databricks/accessConnectors/test-access-connector",
    "name": "test-access-connector",
    "type": "Microsoft.Databricks/accessConnectors",
    "location": "westeurope",
    "identity": {
        "principalId": "test",
        "tenantId": "test",
        "type": "SystemAssigned"
    },
    "tags": {
        "application": "databricks",
        "Owner": "cor.zuurmond@databricks.com",
        "RemoveAfter": "2030-01-01",
        "NoAutoRemove": "True"
    },
    "properties": {
        "provisioningState": "Succeeded"
    },
    "systemData": {
        "createdAt": "0001-01-01T00:00:00+00:00",
        "lastModifiedAt": "0001-01-01T00:00:00+00:00"
    }
}]}

@pytest.fixture
def azure_management_api_client(ws: WorkspaceClient) -> AzureAPIClient:
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    azure_mgmt_client = create_autospec(azure_mgmt_client)
    return azure_mgmt_client


@pytest.fixture
def access_connector_client(azure_management_api_client: AzureAPIClient) -> AccessConnectorClient:
    access_connector_client = AccessConnectorClient(azure_management_api_client)
    return access_connector_client


def test_access_connector_handler_list_access_connectors(
    azure_management_api_client: AzureAPIClient,
    access_connector_client: AccessConnectorClient,
) -> None:
    azure_management_api_client.get.return_value = TEST_ACCESS_CONNECTOR_RESPONSE
    assert len(access_connector_client.list("test")) > 0