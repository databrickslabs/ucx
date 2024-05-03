import logging
from unittest.mock import call, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import InvalidParameterValue, PermissionDenied
from databricks.sdk.service.catalog import (
    AzureManagedIdentityResponse,
    AzureServicePrincipal,
    ExternalLocationInfo,
    StorageCredentialInfo,
)

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.azure.resources import AzureResource, AzureResources, StorageAccount
from databricks.labs.ucx.hive_metastore import ExternalLocations, TablesCrawler, Mounts
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL
from tests.unit.azure import azure_api_client


def location_migration_for_test(ws, mock_backend, mock_installation, azurerm=None):
    azurerm = azurerm or AzureResources(azure_api_client(), azure_api_client())
    location_crawler = ExternalLocations(ws, mock_backend, "location_test")
    azure_resource_permissions = AzureResourcePermissions(mock_installation, ws, azurerm, location_crawler)
    tables_crawler = TablesCrawler(mock_backend, 'ucx')
    mounts_crawler = Mounts(mock_backend, ws, 'ucx')
    principal_acl = PrincipalACL(ws, mock_backend, mock_installation, tables_crawler, mounts_crawler, {})
    external_locations_migration = ExternalLocationsMigration(
        ws, location_crawler, azure_resource_permissions, azurerm, principal_acl
    )
    return external_locations_migration


def test_run_service_principal():
    """test run with service principal based storage credentials"""
    ws = create_autospec(WorkspaceClient)

    # mock crawled HMS external locations
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://container1@test.dfs.core.windows.net/one/", 1),
                ("abfss://container2@test.dfs.core.windows.net/", 2),
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


def test_skip_unsupported_location(caplog):
    # mock crawled HMS external locations with two unsupported locations adl and wasbs
    ws = create_autospec(WorkspaceClient)
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://container1@test.dfs.core.windows.net/one/", 1),
                ("adl://container2@test.dfs.core.windows.net/", 2),
                ("wasbs://container2@test.dfs.core.windows.net/", 2),
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
                    'prefix': 'adl://container2@test.dfs.core.windows.net/',
                    'client_id': 'application_id_1',
                    'principal': 'credential_sp1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
                {
                    'prefix': 'wasbs://container2@test.dfs.core.windows.net/',
                    'client_id': 'application_id_1',
                    'principal': 'credential_sp1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
            ],
        }
    )

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)
    location_migration.run()

    ws.external_locations.create.assert_called_once_with(
        "container1_test_one",
        "abfss://container1@test.dfs.core.windows.net/one/",
        "credential_sp1",
        comment="Created by UCX",
        read_only=False,
        skip_validation=False,
    )
    assert "Skip unsupported location: adl://container2@test.dfs.core.windows.net" in caplog.text
    assert "Skip unsupported location: wasbs://container2@test.dfs.core.windows.net" in caplog.text


def test_run_managed_identity():
    """test run with managed identity based storage credentials"""
    ws = create_autospec(WorkspaceClient)

    # mock crawled HMS external locations
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://container4@test.dfs.core.windows.net/", 4),
                ("abfss://container5@test.dfs.core.windows.net/a/b/", 5),
            ]
        }
    )

    # mock listing storage credentials
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            name="credential_system_assigned_mi",
            azure_managed_identity=AzureManagedIdentityResponse(
                "/subscriptions/123/resourcegroups/abc/providers/Microsoft.Databricks/accessConnectors/credential_system_assigned_mi"
            ),
        ),
        StorageCredentialInfo(
            name="credential_user_assigned_mi",
            azure_managed_identity=AzureManagedIdentityResponse(
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


@pytest.mark.parametrize(
    "azure_storage_account_info",
    [
        list(),
        # Storage credentials based on access connectors take priority over other credentials
        [
            {
                'prefix': 'abfss://container4@test.dfs.core.windows.net/',
                'client_id': 'application_id_system_assigned_mi-123',
                'principal': 'credential_system_assigned_mi',
                'privilege': 'WRITE_FILES',
                'type': 'ManagedIdentity',
            },
            {
                'prefix': 'abfss://container5@test.dfs.core.windows.net/',
                'client_id': 'application_id_1',
                'principal': 'credential_sp1',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'directory_id': 'directory_id_1',
            },
        ],
    ],
)
def test_run_access_connectors(azure_storage_account_info):
    """Test run with access connectors based storage credentials"""
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://container4@test.dfs.core.windows.net/", 4),
                ("abfss://container5@test.dfs.core.windows.net/a/b/", 5),
            ]
        }
    )

    storage_account = StorageAccount(
        id=AzureResource("/subscriptions/002/resourceGroups/rg1/storageAccounts/test"),
        name="test",
        location="eastus",
        default_network_action="Allow",
    )
    azurerm = create_autospec(AzureResources)
    azurerm.storage_accounts.return_value = [storage_account]

    azurerm.containers.return_value = [
        AzureResource(f"{storage_account.id}/containers/container4"),
        AzureResource(f"{storage_account.id}/containers/container5"),
    ]

    ws = create_autospec(WorkspaceClient)
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(name=f"ac-{storage_account.name}", comment="Created by UCX"),
    ]

    ws.external_locations.list.return_value = [ExternalLocationInfo(name="none", url="none")]

    mock_installation = MockInstallation({"azure_storage_account_info.csv": azure_storage_account_info})

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation, azurerm)
    location_migration.run()

    calls = [
        call(
            "container4_test",
            "abfss://container4@test.dfs.core.windows.net/",
            "ac-test",
            comment="Created by UCX",
            read_only=False,
            skip_validation=False,
        ),
        call(
            "container5_test_a_b",
            "abfss://container5@test.dfs.core.windows.net/a/b/",
            "ac-test",
            comment="Created by UCX",
            read_only=False,
            skip_validation=False,
        ),
    ]
    ws.external_locations.create.assert_has_calls(calls)


def test_run_access_connectors_warn_storage_account_not_found(caplog):
    """A warning should be raised when a storage account is not found."""
    storage_account = StorageAccount(
        id=AzureResource("/subscriptions/002/resourceGroups/rg1/storageAccounts/other-storage-account"),
        name="other-storage-account",
        location="eastus",
        default_network_action="Allow",
    )
    azurerm = create_autospec(AzureResources)
    azurerm.storage_accounts.return_value = [storage_account]

    ws = create_autospec(WorkspaceClient)
    ws.storage_credentials.list.return_value = [StorageCredentialInfo(name=f"ac-test", comment="Created by UCX")]

    mock_installation = MockInstallation({"azure_storage_account_info.csv": []})

    location_migration = location_migration_for_test(ws, MockBackend(), mock_installation, azurerm)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.azure.locations"):
        location_migration.run()

    assert any("Storage account test for access connector ac-test not found" in message for message in caplog.messages)
    ws.external_locations.create.assert_not_called()


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


def test_location_failed_to_read():
    """If read-only location is empty, READ permission check will fail with PermissionDenied"""
    ws = create_autospec(WorkspaceClient)

    # mock crawled HMS external locations
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://empty@test.dfs.core.windows.net/", 1),
                ("abfss://other_permission_denied@test.dfs.core.windows.net/", 2),
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


def test_overlapping_locations(caplog):
    caplog.set_level(logging.INFO)
    ws = create_autospec(WorkspaceClient)

    # mock crawled HMS external locations
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://overlap_location@test.dfs.core.windows.net/a/", 1),
                ("abfss://other_invalid_parameter@test.dfs.core.windows.net/a/", 1),
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


def test_corner_cases_with_missing_fields(caplog, mocker):
    """test corner cases with: missing credential name, missing application_id"""
    caplog.set_level(logging.INFO)

    ws = create_autospec(WorkspaceClient)

    # mock crawled HMS external locations
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM location_test.external_locations": MockBackend.rows("location", "table_count")[
                ("abfss://container1@test.dfs.core.windows.net/", 1),
                ("abfss://container2@test.dfs.core.windows.net/", 2),
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
            azure_managed_identity=AzureManagedIdentityResponse(
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

    location_migration = location_migration_for_test(ws, mock_backend, mock_installation)
    location_migration.run()

    ws.external_locations.create.assert_not_called()
    assert "External locations below are not created in UC." in caplog.text
