from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import AzureResource, AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations

from ..framework.mocks import MockBackend
from . import get_az_api_mapping


def test_save_spn_permissions_no_external_table(caplog):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": []}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    azure_resource_permission.save_spn_permissions()
    msg = "There are no external table present with azure storage account. Please check if assessment job is run"
    assert [rec.message for rec in caplog.records if msg in rec.message]


def test_save_spn_permissions_no_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    storage_accounts = azure_resource_permission._get_storage_accounts()
    assert len(storage_accounts) == 0


def test_save_spn_permissions_valid_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {
        "SELECT \\* FROM ucx.external_locations": [
            ["s3://bucket1/folder1", "1"],
            ["abfss://continer1@storage1.dfs.core.windows.net/folder1", "1"],
        ]
    }
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    storage_accounts = azure_resource_permission._get_storage_accounts()
    assert storage_accounts[0] == "storage1"


def test_map_storage_with_spn_no_blob_permission(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3"
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    storage_permission_mappings = azure_resource_permission._map_storage(AzureResource(resource_id))
    assert len(storage_permission_mappings) == 0


def test_get_role_assignments_with_spn_and_blob_permission(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    storage_permission_mappings = azure_resource_permission._map_storage(AzureResource(resource_id))
    assert len(storage_permission_mappings) == 2
    for storage_permission_mapping in storage_permission_mappings:
        assert storage_permission_mapping.prefix == "abfss://container3@sto2.dfs.core.windows.net/"
        assert storage_permission_mapping.principal == "disNameuser3"
        assert storage_permission_mapping.privilege == "WRITE_FILES"
        assert storage_permission_mapping.client_id == "appIduser3"


def test_save_spn_permissions_no_valid_storage_accounts(caplog, mocker, az_token):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://continer1@sto3.dfs.core.windows.net/folder1", 1]]}
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    azure_resource_permission.save_spn_permissions()
    assert [rec.message for rec in caplog.records if "No storage account found in current tenant" in rec.message]


def test_save_spn_permissions_valid_storage_accounts(caplog, mocker, az_token):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://continer1@sto2.dfs.core.windows.net/folder1", 1]]}
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    azure_resource_permission.save_spn_permissions()
    installation.assert_file_written(
        'azure_storage_account_info.csv',
        [
            {
                'client_id': 'appIduser3',
                'prefix': 'abfss://container3@sto2.dfs.core.windows.net/',
                'principal': 'disNameuser3',
                'privilege': 'WRITE_FILES',
            },
            {
                'client_id': 'appIduser3',
                'prefix': 'abfss://container3@sto2.dfs.core.windows.net/',
                'principal': 'disNameuser3',
                'privilege': 'WRITE_FILES',
            },
        ],
    )
