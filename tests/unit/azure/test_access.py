from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
    AzureRoleAssignment,
    Principal,
)
from databricks.labs.ucx.hive_metastore import ExternalLocations

from ..framework.mocks import MockBackend


def test_save_spn_permissions_no_external_table(caplog):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": []}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    api_client = AzureAPIClient(w)
    azure_resources = create_autospec(AzureResources(api_client=api_client))
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resources.storage_accounts.return_value = []
    azure_resource_permission.save_spn_permissions()
    msg = "There are no external table present with azure storage account. Please check if assessment job is run"
    assert [rec.message for rec in caplog.records if msg in rec.message]


def test_save_spn_permissions_no_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    api_client = AzureAPIClient(w)
    azure_resources = create_autospec(AzureResources(api_client=api_client))
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
            principal=Principal('a', 'b', 'c', '0000-0000'),
            role_name='Storage Blob Data Contributor',
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('d', 'e', 'f', '0000-0000'),
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
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'directory_id': '0000-0000',
            },
        ],
    )
