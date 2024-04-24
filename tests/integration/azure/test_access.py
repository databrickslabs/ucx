import json
import logging
import os
import sys

import pytest
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)

from databricks.sdk.errors.platform import PermissionDenied

logger = logging.getLogger(__name__)


def test_azure_storage_accounts(ws, sql_backend, inventory_schema, make_random):
    # skip this test if not in local mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        return
    tables = [
        ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random(4))
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(azure_mgmt_client, graph_client)
    az_res_perm = AzureResourcePermissions(installation, ws, azure_resources, location)
    az_res_perm.save_spn_permissions()
    mapping = az_res_perm.load()
    assert mapping[0].prefix == "abfss://things@labsazurethings.dfs.core.windows.net/"


def test_save_spn_permissions_local(ws, sql_backend, inventory_schema, make_random):
    # skip this test if not in local mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        return
    tables = [
        ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random(4))
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(azure_mgmt_client, graph_client)
    az_res_perm = AzureResourcePermissions(installation, ws, azure_resources, location)
    path = az_res_perm.save_spn_permissions()
    assert ws.workspace.get_status(path)


@pytest.fixture
def clean_up_spn():
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    yield
    spns = graph_client.get("/v1.0/applications?$filter=startswith(displayName, 'UCXServicePrincipal')")['value']
    logging.debug("clearing ucx uber service principals")
    for spn in spns:
        try:
            graph_client.delete(f"/v1.0/applications(appId='{spn['appId']}')")
        except PermissionDenied:
            continue


def test_create_global_spn(
    ws, sql_backend, inventory_schema, make_random, make_cluster_policy, env_or_skip, clean_up_spn
):
    # skip this test if not in local mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        return
    tables = [
        ExternalLocation(f"{env_or_skip('TEST_MOUNT_CONTAINER')}/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    installation = Installation(ws, make_random(4))
    policy = make_cluster_policy()
    installation.save(WorkspaceConfig(inventory_database='ucx', policy_id=policy.policy_id))
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azure_resources = AzureResources(azure_mgmt_client, graph_client)
    az_res_perm = AzureResourcePermissions(
        installation, ws, azure_resources, ExternalLocations(ws, sql_backend, inventory_schema)
    )
    az_res_perm.create_uber_principal(
        MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})
    )
    config = installation.load(WorkspaceConfig)
    assert config.uber_spn_id is not None
    policy_definition = json.loads(ws.cluster_policies.get(policy_id=policy.policy_id).definition)
    role_assignments = azure_resources.role_assignments(env_or_skip("TEST_STORAGE_RESOURCE"))
    global_spn_assignment = None
    for assignment in role_assignments:
        if assignment.principal.client_id == config.uber_spn_id:
            global_spn_assignment = assignment
            break
    assert global_spn_assignment
    assert global_spn_assignment.principal.client_id == config.uber_spn_id
    assert global_spn_assignment.role_name == "Storage Blob Data Contributor"
    assert str(global_spn_assignment.scope) == env_or_skip("TEST_STORAGE_RESOURCE")
    assert (
        policy_definition["spark_conf.fs.azure.account.oauth2.client.id.labsazurethings.dfs.core.windows.net"]["value"]
        == config.uber_spn_id
    )
    assert (
        policy_definition["spark_conf.fs.azure.account.oauth2.client.endpoint.labsazurethings.dfs.core.windows.net"][
            "value"
        ]
        == "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"
    )
