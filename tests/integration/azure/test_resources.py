import pytest

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.azure.resources import AzureAPIClient


@pytest.fixture
def azure_management_api_client(ws: WorkspaceClient) -> AzureAPIClient:
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    return azure_mgmt_client
