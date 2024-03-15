import logging
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import InvalidParameterValue, PermissionDenied
from databricks.sdk.service.catalog import (
    AzureManagedIdentity,
    AzureServicePrincipal,
    ExternalLocationInfo,
    StorageCredentialInfo,
)

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from tests.unit.azure import get_az_api_mapping


@pytest.fixture
def ws():
    return create_autospec(WorkspaceClient)


def location_migration_for_test(ws, mock_backend, mock_installation):
    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azurerm = AzureResources(azure_mgmt_client, graph_client)

    location_crawler = ExternalLocations(ws, mock_backend, "location_test")

    return ExternalLocationsMigration(
        ws, location_crawler, AzureResourcePermissions(mock_installation, ws, azurerm, location_crawler), azurerm
    )


def test_run_service_principal(ws):
    """test run with service principal based storage credentials"""

    # mock crawled HMS external locations
    row_factory = type("Row", (Row,), {"__columns__": ["location", "table_count"]})
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": [
                row_factory(["abfss://container1@test.dfs.core.windows.net/one/", 1]),
                row_factory(["abfss://container2@test.dfs.core.windows.net/", 2]),
            ]
        }
    )

    # mock listing storage credentials
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            name="credential_sp1",
            azure_service_principal=AzureServicePrincipal(
                "directory_id_1",
                "application_id_1",
                "test_secret",
            ),
        ),
        StorageCredentialInfo(
            name="credential_sp2",
            azure_service_principal=AzureServicePrincipal("directory_id_2", "application_id_2", "test_secret"),
            read_only=True,
        ),
    ]

    # mock listing UC external locations, no HMS external location will be matched
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="none", url="none")]

    # mock installation with permission mapping
    mock_installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://container1@test.dfs.core.windows.net/',
                    'client_id': 'application_id_1',
                    'principal': 'credential_sp1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
                {
                    'prefix': 'abfss://container2@test.dfs.core.windows.net/',
                    'client_id': 'application_id_2',
                    'principal': 'credential_sp2',
                    'privilege': 'READ_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
            ],
        }
    )

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)
    location_migration.run()

    ws.external_locations.create.assert_any_call(
        "container1_test_one",
        "abfss://container1@test.dfs.core.windows.net/one/",
        "credential_sp1",
        comment="Created by UCX",
        read_only=False,
        skip_validation=False,
    )
    ws.external_locations.create.assert_any_call(
        "container2_test",
        "abfss://container2@test.dfs.core.windows.net/",
        "credential_sp2",
        comment="Created by UCX",
        read_only=True,
        skip_validation=False,
    )


def test_run_managed_identity(ws, mocker):
    """test run with managed identity based storage credentials"""

    # mock crawled HMS external locations
    row_factory = type("Row", (Row,), {"__columns__": ["location", "table_count"]})
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": [
                row_factory(["abfss://container4@test.dfs.core.windows.net/", 4]),
                row_factory(["abfss://container5@test.dfs.core.windows.net/a/b/", 5]),
            ]
        }
    )

    # mock listing storage credentials
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            name="credential_system_assigned_mi",
            azure_managed_identity=AzureManagedIdentity(
                "/subscriptions/123/resourcegroups/abc/providers/Microsoft.Databricks/accessConnectors/credential_system_assigned_mi"
            ),
        ),
        StorageCredentialInfo(
            name="credential_user_assigned_mi",
            azure_managed_identity=AzureManagedIdentity(
                "/subscriptions/123/resourcegroups/abc/providers/Microsoft.ManagedIdentity/accessConnectors/credential_user_assigned_mi",
                managed_identity_id="/subscriptions/123/resourceGroups/abc/providers/Microsoft.ManagedIdentity/userAssignedIdentities/credential_user_assigned_mi",
            ),
            read_only=True,
        ),
    ]

    # mock listing UC external locations, no HMS external location will be matched
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="none", url="none")]

    # mock installation with permission mapping
    mock_installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://container4@test.dfs.core.windows.net/',
                    'client_id': 'application_id_system_assigned_mi-123',
                    'principal': 'credential_system_assigned_mi',
                    'privilege': 'WRITE_FILES',
                    'type': 'ManagedIdentity',
                },
                {
                    'prefix': 'abfss://container5@test.dfs.core.windows.net/',
                    'client_id': 'application_id_user_assigned_mi-123',
                    'principal': 'credential_user_assigned_mi',
                    'privilege': 'READ_FILES',
                    'type': 'ManagedIdentity',
                },
            ],
        }
    )

    # mock Azure resource manager and graph API calls for getting application_id of managed identity
    # TODO: (qziyuan) use a better way to mock the API calls
    # pylint: disable-next=prohibited-patch
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)
    location_migration.run()

    ws.external_locations.create.assert_any_call(
        "container4_test",
        "abfss://container4@test.dfs.core.windows.net/",
        "credential_system_assigned_mi",
        comment="Created by UCX",
        read_only=False,
        skip_validation=False,
    )
    ws.external_locations.create.assert_any_call(
        "container5_test_a_b",
        "abfss://container5@test.dfs.core.windows.net/a/b/",
        "credential_user_assigned_mi",
        comment="Created by UCX",
        read_only=True,
        skip_validation=False,
    )


def create_side_effect(location_name, *args, **kwargs):  # pylint: disable=unused-argument
    # if external_locations.create is called without skip_validation=True, raise PermissionDenied
    if not kwargs.get('skip_validation'):
        if "empty" in location_name:
            raise PermissionDenied("No file available under the location to read")
        if "other_permission_denied" in location_name:
            raise PermissionDenied("Other PermissionDenied exception")
        if "overlap_location" in location_name:
            raise InvalidParameterValue("overlaps with an existing external location")
        if "other_invalid_parameter" in location_name:
            raise InvalidParameterValue("Other InvalidParameterValue exception")


def test_location_failed_to_read(ws):
    """If read-only location is empty, READ permission check will fail with PermissionDenied"""

    # mock crawled HMS external locations
    row_factory = type("Row", (Row,), {"__columns__": ["location", "table_count"]})
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": [
                row_factory(["abfss://empty@test.dfs.core.windows.net/", 1]),
                row_factory(["abfss://other_permission_denied@test.dfs.core.windows.net/", 2]),
            ]
        }
    )
    # mock listing storage credentials
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            name="credential_sp2",
            azure_service_principal=AzureServicePrincipal("directory_id_2", "application_id_2", "test_secret"),
            read_only=True,
        ),
    ]
    # mock listing UC external locations, no HMS external location will be matched
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="none", url="none")]

    # mock installation with permission mapping
    mock_installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://empty@test.dfs.core.windows.net/',
                    'client_id': 'application_id_2',
                    'principal': 'credential_sp2',
                    'privilege': 'READ_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
                {
                    'prefix': 'abfss://other_permission_denied@test.dfs.core.windows.net/',
                    'client_id': 'application_id_2',
                    'principal': 'credential_sp2',
                    'privilege': 'READ_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
            ],
        }
    )

    # make external_locations.create to raise PermissionDenied when first called to create read-only external location.
    ws.external_locations.create.side_effect = create_side_effect

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)

    # assert PermissionDenied got re-threw if the exception
    with pytest.raises(PermissionDenied):
        location_migration.run()

    # assert the PermissionDenied due to empty location is handled and external_locations.create is called again with skip_validation=True
    ws.external_locations.create.assert_any_call(
        "empty_test",
        "abfss://empty@test.dfs.core.windows.net/",
        "credential_sp2",
        comment="Created by UCX",
        read_only=True,
        skip_validation=True,
    )


def test_overlapping_locations(ws, caplog):
    caplog.set_level(logging.INFO)

    # mock crawled HMS external locations
    row_factory = type("Row", (Row,), {"__columns__": ["location", "table_count"]})
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": [
                row_factory(["abfss://overlap_location@test.dfs.core.windows.net/a/", 1]),
                row_factory(["abfss://other_invalid_parameter@test.dfs.core.windows.net/a/", 1]),
            ]
        }
    )

    # mock listing storage credentials
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            name="credential_sp1",
            azure_service_principal=AzureServicePrincipal(
                "directory_id_1",
                "application_id_1",
                "test_secret",
            ),
        )
    ]

    # mock listing UC external locations, a location that is sub path of prefix is already created
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="none", url="none")]

    # mock installation with permission mapping
    mock_installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://overlap_location@test.dfs.core.windows.net/',
                    'client_id': 'application_id_1',
                    'principal': 'credential_sp1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
                {
                    'prefix': 'abfss://other_invalid_parameter@test.dfs.core.windows.net/',
                    'client_id': 'application_id_1',
                    'principal': 'credential_sp1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
            ],
        }
    )

    ws.external_locations.create.side_effect = create_side_effect

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)

    # assert InvalidParameterValue got re-threw if it's not caused by overlapping location
    with pytest.raises(InvalidParameterValue):
        location_migration.run()
    # assert the InvalidParameterValue due to overlapping location is handled.
    assert "overlaps with an existing external location" in caplog.text


def test_corner_cases_with_missing_fields(ws, caplog, mocker):
    """test corner cases with: missing credential name, missing application_id"""
    caplog.set_level(logging.INFO)

    # mock crawled HMS external locations
    row_factory = type("Row", (Row,), {"__columns__": ["location", "table_count"]})
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": [
                row_factory(["abfss://container1@test.dfs.core.windows.net/", 1]),
                row_factory(["abfss://container2@test.dfs.core.windows.net/", 2]),
            ]
        }
    )

    # mock listing storage credentials
    ws.storage_credentials.list.return_value = [
        # credential without name
        StorageCredentialInfo(
            azure_service_principal=AzureServicePrincipal(
                "directory_id_1",
                "application_id_1",
                "test_secret",
            ),
        ),
        StorageCredentialInfo(
            name="credential_no_id_mi",
            azure_managed_identity=AzureManagedIdentity(
                "/subscriptions/123/no_id_test/accessConnectors/credential_no_id_mi",
            ),
        ),
    ]

    # mock listing UC external locations, no HMS external location will be matched
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="none", url="none")]

    # mock installation with permission mapping
    mock_installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'dummy',
                    'client_id': 'dummy',
                    'principal': 'dummy',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'dummy',
                },
            ],
        }
    )
    # return None when getting application_id of managed identity
    # TODO: (qziyuan) use a better way to mock the API calls
    # pylint: disable-next=prohibited-patch
    mocker.patch("databricks.sdk.core.ApiClient.do", return_value={"dummy": "dummy"})

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)
    location_migration.run()

    ws.external_locations.create.assert_not_called()
    assert "External locations below are not created in UC." in caplog.text


def test_for_cli(ws):
    mock_installation = MockInstallation(
        {
            "config.yml": {
                'version': 2,
                'inventory_database': 'test',
                'connect': {
                    'host': 'test',
                    'token': 'test',
                },
            },
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'dummy',
                    'client_id': 'dummy',
                    'principal': 'dummy',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'dummy',
                },
            ],
        }
    )
    assert isinstance(ExternalLocationsMigration.for_cli(ws, mock_installation), ExternalLocationsMigration)
