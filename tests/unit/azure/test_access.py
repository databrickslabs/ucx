import json
from unittest.mock import call, create_autospec

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import Policy

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import (
    AzureResource,
    AzureResources,
    AzureRoleAssignment,
    Principal,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations

from ..framework.mocks import MockBackend
from . import azure_api_client


def test_save_spn_permissions_no_external_table(caplog):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": []}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resources.storage_accounts.return_value = []
    azure_resource_permission.save_spn_permissions()
    msg = "There are no external table present with azure storage account. Please check if assessment job is run"
    assert [rec.message for rec in caplog.records if msg in rec.message]


def test_save_spn_permissions_no_external_tables():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resources.storage_accounts.return_value = []
    assert not azure_resource_permission.save_spn_permissions()


def test_save_spn_permissions_no_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {
        "SELECT \\* FROM ucx.external_locations": [["abfss://container1@storage1.dfs.core.windows.net/folder1", "1"]]
    }
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resources.storage_accounts.return_value = []
    assert not azure_resource_permission.save_spn_permissions()


def test_save_spn_permissions_valid_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {
        "SELECT \\* FROM ucx.external_locations": [
            ["s3://bucket1/folder1", "1"],
            ["abfss://container1@storage1.dfs.core.windows.net/folder1", "1"],
        ]
    }
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resources = create_autospec(AzureResources)
    storage_accounts = '/subscriptions/abc/providers/Microsoft.Storage/storageAccounts'
    containers = f'{storage_accounts}/storage1/blobServices/default/containers'
    azure_resources.storage_accounts.return_value = [
        AzureResource(f'{storage_accounts}/storage1'),
        AzureResource(f'{storage_accounts}/storage2'),
    ]
    azure_resources.containers.return_value = [
        AzureResource(f'{containers}/container1'),
        AzureResource(f'{containers}/container2'),
    ]
    azure_resources.role_assignments.return_value = [
        AzureRoleAssignment(
            resource=AzureResource(f'{containers}/container1'),
            scope=AzureResource(f'{containers}/container1'),
            principal=Principal('a', 'b', 'c', 'Application', '0000-0000'),
            role_name='Storage Blob Data Contributor',
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('d', 'e', 'f', 'Application', '0000-0000'),
            role_name='Button Clicker',
        ),
    ]
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resource_permission.save_spn_permissions()
    installation.assert_file_written(
        'azure_storage_account_info.csv',
        [
            {
                'client_id': 'a',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'directory_id': '0000-0000',
            },
        ],
    )


def test_create_global_spn_no_policy():
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend, "ucx")
    installation = create_autospec(Installation)
    installation.load.return_value = WorkspaceConfig(inventory_database='ucx')
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    with pytest.raises(ValueError):
        azure_resource_permission.create_uber_principal()


def test_create_global_spn_spn_present():
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend, "ucx")
    installation = create_autospec(Installation)
    installation.load.return_value = WorkspaceConfig(
        inventory_database='ucx',
        policy_id="foo1",
        global_spn_id="123",
    )
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    assert not azure_resource_permission.create_uber_principal()


def test_create_global_spn_no_storage():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = create_autospec(Installation)
    installation.load.return_value = WorkspaceConfig(inventory_database='ucx', policy_id="foo1")
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    assert not azure_resource_permission.create_uber_principal()


def test_create_global_spn_cluster_policy_not_found(mocker):
    w = create_autospec(WorkspaceClient)
    w.cluster_policies.get.side_effect = NotFound()
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://container1@sto2.dfs.core.windows.net/folder1", "1"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = create_autospec(Installation)
    installation.load.return_value = WorkspaceConfig(inventory_database='ucx', policy_id="foo1")
    api_client = azure_api_client(mocker)
    azure_resources = AzureResources(api_client, api_client, include_subscriptions="002")
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    with pytest.raises(NotFound):
        azure_resource_permission.create_uber_principal()


def test_create_global_spn(mocker):
    w = create_autospec(WorkspaceClient)
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    w.cluster_policies.get.return_value = cluster_policy
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://container1@sto2.dfs.core.windows.net/folder1", "1"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = create_autospec(Installation)
    installation.load.return_value = WorkspaceConfig(inventory_database='ucx', policy_id="foo1")
    api_client = azure_api_client(mocker)
    azure_resources = AzureResources(api_client, api_client, include_subscriptions="002")
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resource_permission.create_uber_principal()
    call_1 = call(
        WorkspaceConfig(
            inventory_database='ucx',
            policy_id="foo1",
            global_spn_id="appIduser1",
        )
    )
    # call_2 = call(cluster_policy.as_dict(), filename='policy-backup.json')
    installation.save.assert_has_calls([call_1])
    path = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/providers/Microsoft.Authorization/roleAssignments/12345"
    body = {
        'properties': {
            'principalId': 'Iduser1',
            'principalType': 'ServicePrincipal',
            'roleDefinitionId': '/subscriptions/002/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1',
        }
    }
    call_1 = call("/v1.0/applications", {"displayName": "UCXServicePrincipal"})
    call_2 = call("/v1.0/servicePrincipals", {"appId": "appIduser1"})
    call_3 = call("/v1.0/servicePrincipals/Iduser1/addPassword")
    call_4 = call(path, "2022-04-01", body)
    api_client.post.assert_has_calls([call_1, call_2, call_3], any_order=True)
    api_client.put.assert_has_calls([call_4], any_order=True)
    definition = {
        "foo": "bar",
        "spark_conf.fs.azure.account.oauth2.client.id.sto2.dfs.core.windows.net": {
            "type": "fixed",
            "value": "appIduser1",
        },
        "spark_conf.fs.azure.account.oauth.provider.type.sto2.dfs.core.windows.net": {
            "type": "fixed",
            "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        },
        "spark_conf.fs.azure.account.oauth2.client.endpoint.sto2.dfs.core.windows.net": {
            "type": "fixed",
            "value": "https://login.microsoftonline.com/bar/oauth2/token",
        },
        "spark_conf.fs.azure.account.auth.type.sto2.dfs.core.windows.net": {"type": "fixed", "value": "OAuth"},
        "spark_conf.fs.azure.account.oauth2.client.secret.sto2.dfs.core.windows.net": {
            "type": "fixed",
            "value": "mypwd",
        },
    }
    w.cluster_policies.edit.assert_called_with(
        'foo1', 'Unity Catalog Migration (ucx) (me@example.com)', definition=json.dumps(definition)
    )
