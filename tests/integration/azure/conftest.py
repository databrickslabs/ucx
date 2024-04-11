import pytest

from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources


@pytest.fixture
def azure_resources(ws):
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(azure_mgmt_client, graph_client)
    return azure_resources
