import logging

from databricks.labs.blueprint.installation import Installation

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)


# @pytest.mark.skip
def test_azure_storage_accounts(ws, sql_backend, inventory_schema, make_random):
    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")
    tables = [
        ExternalLocation("abfss://data@hsucxstorage.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random(4))
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(
        azure_mgmt_client, graph_client, include_subscriptions="3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"
    )
    az_res_perm = AzureResourcePermissions(installation, ws, azure_resources, location)
    az_res_perm.save_spn_permissions()

    mapping = az_res_perm.load()
    # assert len(mapping) == 1
    assert mapping[0].prefix == "hsucxstorage"


# @pytest.mark.skip
def test_save_spn_permissions_local(ws, sql_backend, inventory_schema, make_random):
    tables = [
        ExternalLocation("abfss://data@hsucxstorage.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random(4))
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(
        azure_mgmt_client, graph_client, include_subscriptions="3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"
    )
    az_res_perm = AzureResourcePermissions(installation, ws, azure_resources, location)
    path = az_res_perm.save_spn_permissions()
    assert ws.workspace.get_status(path)


def test_create_global_spn(ws, sql_backend, inventory_schema, make_random, make_cluster_policy):
    tables = [
        ExternalLocation("abfss://data@hsucxstorage.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random(4))
    policy = make_cluster_policy()
    installation.save(WorkspaceConfig(inventory_database='ucx', policy_id=policy.policy_id))
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(
        azure_mgmt_client, graph_client, include_subscriptions="3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"
    )
    az_res_perm = AzureResourcePermissions(installation, ws, azure_resources, location)
    az_res_perm.create_uber_principal()
    config = installation.load(WorkspaceConfig)
    assert config.global_spn_id is not None
    resource_id = "/subscriptions/3f2e4d32-8e8d-46d6-82bc-5bb8d962328b/resourceGroups/HSRG/providers/Microsoft.Storage/storageAccounts/hsucxstorage"
    role_assignments = azure_resources.role_assignments(resource_id)
    global_spn_assignment = None
    for assignment in role_assignments:
        if assignment.principal.client_id == config.global_spn_id:
            global_spn_assignment = assignment
            break
    assert global_spn_assignment
    assert global_spn_assignment.principal.client_id == config.global_spn_id
    assert global_spn_assignment.role_name == "READ_FILES"
    assert global_spn_assignment.scope == resource_id


def test_role_assignment(ws, sql_backend, inventory_schema, make_random, make_cluster_policy):

    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(
        azure_mgmt_client, graph_client, include_subscriptions="3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"
    )
    azure_r = AzureResource(
        "/subscriptions/3f2e4d32-8e8d-46d6-82bc-5bb8d962328b/resourceGroups/HSRG/providers/Microsoft.Storage/storageAccounts/hsucxstorage"
    )
    azure_resources.apply_storage_permission("99a5f8b1-8402-4968-b6da-35cb3192a729", azure_r, "STORAGE_BLOB_READER")
