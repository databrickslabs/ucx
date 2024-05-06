import json
import logging
from unittest.mock import call, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import Policy
from databricks.sdk.service.workspace import GetSecretResponse

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import (
    AccessConnector,
    AzureResource,
    AzureResources,
    AzureRoleAssignment,
    Principal,
    StorageAccount,
)
from databricks.labs.ucx.hive_metastore import ExternalLocations

from . import azure_api_client


def test_save_spn_permissions_no_external_table(caplog):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": []}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resources = create_autospec(AzureResources)
    azure_resources.storage_accounts.return_value = []
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resource_permission.save_spn_permissions()
    msg = "There are no external table present with azure storage account. Please check if assessment job is run"
    assert [rec.message for rec in caplog.records if msg in rec.message]
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_not_called()


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
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_not_called()


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
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_not_called()


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
        StorageAccount(
            id=AzureResource(f'{storage_accounts}/storage1'),
            name="storage1",
            location="westeu",
            default_network_action="Allow",
        ),
        StorageAccount(
            id=AzureResource(f'{storage_accounts}/storage2'),
            name="storage2",
            location="westeu",
            default_network_action="Allow",
        ),
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
            role_type='BuiltInRole',
            role_permissions=[],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('d', 'e', 'f', 'Application', '0000-0000'),
            role_name='Button Clicker',
            role_type='BuiltInRole',
            role_permissions=[],
        ),
    ]
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resource_permission.save_spn_permissions()
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_not_called()
    installation.assert_file_written(
        'azure_storage_account_info.csv',
        [
            {
                'client_id': 'a',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
        ],
    )


def test_save_spn_permissions_custom_role_valid_azure_storage_account():
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
        StorageAccount(
            id=AzureResource(f'{storage_accounts}/storage1'),
            name="storage1",
            location="westeu",
            default_network_action="Allow",
        ),
        StorageAccount(
            id=AzureResource(f'{storage_accounts}/storage2'),
            name="storage2",
            location="westeu",
            default_network_action="Allow",
        ),
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
            role_name='Custom_role_1',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/blobServices/containers/read",
                "Microsoft.Storage/storageAccounts/blobServices/containers/write",
                "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete",
                "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
                "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('d', 'e', 'f', 'Application', '0000-0000'),
            role_name='Custom_role_2',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/blobServices/containers/read",
                "Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey/action",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('d', 'e', 'f', 'Application', '0000-0000'),
            role_name='Custom_role_2',
            role_type='CustomRole',
            role_permissions=["Microsoft.Authorization/*"],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('g', 'h', 'i', 'Application', '0000-0000'),
            role_name='Custom_role_3',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/blobServices/containers/*",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('j', 'k', 'l', 'Application', '0000-0000'),
            role_name='Custom_role_9',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('j', 'k', 'l', 'Application', '0000-0000'),
            role_name='Custom_role_4',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/*/read",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('m', 'n', 'o', 'Application', '0000-0000'),
            role_name='Custom_role_5',
            role_type='CustomRole',
            role_permissions=[
                "*/write",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('v', 'w', 'x', 'Application', '0000-0000'),
            role_name='Custom_role_6',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/*/containers/*",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('p', 'q', 'r', 'Application', '0000-0000'),
            role_name='Custom_role_7',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Authorization/*",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('s', 't', 'u', 'Application', '0000-0000'),
            role_name='Custom_role_8',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/*/read",
                "*/write",
                "Microsoft.Storage/storageAccounts/*/containers/*",
                "Microsoft.Authorization/*",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('a1', 'b1', 'c1', 'Application', '0000-0000'),
            role_name='Storage Blob Data Contributor',
            role_type='BuiltInRole',
            role_permissions=[],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('a1', 'b1', 'c1', 'Application', '0000-0000'),
            role_name='Custom_role_11',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/*/read",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('a2', 'b2', 'c2', 'Application', '0000-0000'),
            role_name='Custom_role_12',
            role_type='CustomRole',
            role_permissions=[
                "Microsoft.Storage/storageAccounts/*/write",
            ],
        ),
        AzureRoleAssignment(
            resource=AzureResource(f'{storage_accounts}/storage1'),
            scope=AzureResource(f'{storage_accounts}/storage1'),
            principal=Principal('a2', 'b2', 'c2', 'Application', '0000-0000'),
            role_name='Storage Blob Data Reader',
            role_type='BuiltInRole',
            role_permissions=[],
        ),
    ]
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resource_permission.save_spn_permissions()
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_not_called()
    installation.assert_file_written(
        'azure_storage_account_info.csv',
        [
            {
                'client_id': 'a',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'd',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'e',
                'privilege': 'READ_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'g',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'h',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'j',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'k',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'm',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'n',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'v',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'w',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 's',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 't',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a1',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'b1',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a2',
                'prefix': 'abfss://container1@storage1.dfs.core.windows.net/',
                'principal': 'b2',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'b',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'd',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'e',
                'privilege': 'READ_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'g',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'h',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'j',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'k',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'm',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'n',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'v',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'w',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 's',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 't',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a1',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'b1',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
            {
                'client_id': 'a2',
                'prefix': 'abfss://container2@storage1.dfs.core.windows.net/',
                'principal': 'b2',
                'privilege': 'WRITE_FILES',
                'type': 'Application',
                'default_network_action': 'Allow',
                'directory_id': '0000-0000',
            },
        ],
    )


def test_create_global_spn_no_policy():
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        }
    )
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})
    with pytest.raises(ValueError):
        azure_resource_permission.create_uber_principal(prompts)
    azure_resources.storage_accounts.assert_not_called()
    azure_resources.create_or_update_access_connector.assert_not_called()
    azure_resources.role_assignments.assert_not_called()
    azure_resources.containers.assert_not_called()
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_called_once()


def test_create_global_spn_spn_present():
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx',
                'policy_id': 'foo1',
                'uber_spn_id': '123',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        }
    )
    azure_resources = create_autospec(AzureResources)
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    assert not azure_resource_permission.create_uber_principal(prompts)
    azure_resources.storage_accounts.assert_not_called()
    azure_resources.create_or_update_access_connector.assert_not_called()
    azure_resources.role_assignments.assert_not_called()
    azure_resources.containers.assert_not_called()
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_called_once()


def test_create_global_spn_no_storage():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx',
                'policy_id': 'foo1',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        }
    )
    location = ExternalLocations(w, backend, "ucx")
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})
    azure_resources = create_autospec(AzureResources)
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    assert not azure_resource_permission.create_uber_principal(prompts)
    azure_resources.storage_accounts.assert_not_called()
    azure_resources.create_or_update_access_connector.assert_not_called()
    azure_resources.role_assignments.assert_not_called()
    azure_resources.containers.assert_not_called()
    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    w.secrets.put_secret.assert_not_called()
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_called_once()


def test_create_global_spn_cluster_policy_not_found():
    w = create_autospec(WorkspaceClient)
    w.cluster_policies.get.side_effect = NotFound()
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://container1@sto2.dfs.core.windows.net/folder1", "1"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx',
                'policy_id': 'foo1',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        }
    )
    api_client = azure_api_client()
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})
    azure_resources = AzureResources(api_client, api_client, include_subscriptions="002")
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    with pytest.raises(NotFound):
        azure_resource_permission.create_uber_principal(prompts)
    w.cluster_policies.get.assert_called_once()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_called_with("ucx")
    w.secrets.put_secret.assert_called_with('ucx', 'uber_principal_secret', string_value='mypwd')
    w.cluster_policies.edit.assert_not_called()
    w.get_workspace_id.assert_called_once()


def test_create_global_spn():
    w = create_autospec(WorkspaceClient)
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    w.cluster_policies.get.return_value = cluster_policy
    w.secrets.get_secret.return_value = GetSecretResponse("uber_principal_secret", "mypwd")
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://container1@sto2.dfs.core.windows.net/folder1", "1"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx',
                'policy_id': 'foo1',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        }
    )
    api_client = azure_api_client()
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})
    azure_resources = AzureResources(api_client, api_client, include_subscriptions="002")
    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)
    azure_resource_permission.create_uber_principal(prompts)
    installation.assert_file_written(
        'policy-backup.json',
        {'definition': '{"foo": "bar"}', 'name': 'Unity Catalog Migration (ucx) (me@example.com)', 'policy_id': 'foo'},
    )
    call_1 = call("/v1.0/applications", {"displayName": "UCXServicePrincipal"})
    call_2 = call("/v1.0/servicePrincipals", {"appId": "appIduser1"})
    call_3 = call("/v1.0/servicePrincipals/Iduser1/addPassword")
    api_client.post.assert_has_calls([call_1, call_2, call_3], any_order=True)
    api_client.put.assert_called_once()
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
            "value": "{secrets/ucx/uber_principal_secret}",
        },
    }
    w.cluster_policies.edit.assert_called_with(
        'foo1', 'Unity Catalog Migration (ucx) (me@example.com)', definition=json.dumps(definition)
    )
    w.secrets.create_scope.assert_called_with("ucx")
    w.secrets.put_secret.assert_called_with("ucx", "uber_principal_secret", string_value="mypwd")


def test_create_access_connectors_for_storage_accounts_logs_no_storage_accounts(caplog):
    """A warning should be logged when no storage account is present."""
    w = create_autospec(WorkspaceClient)
    backend = MockBackend()
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()

    azure_resources = create_autospec(AzureResources)
    azure_resources.storage_accounts.return_value = []

    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)

    azure_resource_permission.create_access_connectors_for_storage_accounts()

    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
    assert (
        "There are no external table present with azure storage account. Please check if assessment job is run"
        in caplog.messages
    )


@pytest.mark.parametrize("yield_container", (True, False))
def test_create_access_connectors_for_storage_accounts_one_access_connector(yield_container):
    """One access connector should be created for one storage account."""
    w = create_autospec(WorkspaceClient)

    rows = {
        "SELECT \\* FROM ucx.external_locations": [["abfss://container1@storage1.dfs.core.windows.net/folder1", "1"]]
    }
    backend = MockBackend(rows=rows)

    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()

    azure_resources = create_autospec(AzureResources)
    storage_account = StorageAccount(
        id=AzureResource('/subscriptions/abc/providers/Microsoft.Storage/storageAccounts/storage1'),
        name="storage1",
        location="westeu",
        default_network_action="Allow",
    )
    azure_resources.storage_accounts.return_value = [storage_account]

    container = AzureResource(
        "/subscriptions/abc/providers/Microsoft.Storage/storageAccounts/storage1/containers/container"
    )
    if yield_container:
        container_iter = iter([container])
    else:
        container_iter = iter([])
    azure_resources.containers.return_value = container_iter

    access_connector_id = AzureResource(
        "/subscriptions/test/resourceGroups/rg-test/providers/Microsoft.Databricks/accessConnectors/ac-test"
    )
    azure_resources.create_or_update_access_connector.return_value = AccessConnector(
        id=access_connector_id,
        name="ac-test",
        location="westeu",
        provisioning_state="Succeeded",
        identity_type="SystemAssigned",
        principal_id="test",
        tenant_id="test",
    )

    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)

    access_connectors = azure_resource_permission.create_access_connectors_for_storage_accounts()

    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()

    azure_resources.create_or_update_access_connector.assert_called_once()
    azure_resources.containers.assert_called_once_with(storage_account.id)

    assert len(access_connectors) == 1
    assert access_connectors[0][0].name == "ac-test"


def test_create_access_connectors_for_storage_accounts_log_permission_applied(caplog):
    """Log that the permissions for the access connector are applied."""
    w = create_autospec(WorkspaceClient)

    rows = {
        "SELECT \\* FROM ucx.external_locations": [["abfss://container1@storage1.dfs.core.windows.net/folder1", "1"]]
    }
    backend = MockBackend(rows=rows)

    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()

    azure_resources = create_autospec(AzureResources)
    azure_resources.storage_accounts.return_value = [
        StorageAccount(
            id=AzureResource('/subscriptions/abc/providers/Microsoft.Storage/storageAccounts/storage1'),
            name="storage1",
            location="westeu",
            default_network_action="Allow",
        )
    ]

    access_connector_id = AzureResource(
        "/subscriptions/test/resourceGroups/rg-test/providers/Microsoft.Databricks/accessConnectors/ac-test"
    )
    azure_resources.create_or_update_access_connector.return_value = AccessConnector(
        id=access_connector_id,
        name="ac-test",
        location="westeu",
        provisioning_state="Succeeded",
        identity_type="SystemAssigned",
        principal_id="test",
        tenant_id="test",
    )

    azure_resource_permission = AzureResourcePermissions(installation, w, azure_resources, location)

    with caplog.at_level(logging.DEBUG, logger="databricks.labs.ucx"):
        azure_resource_permission.create_access_connectors_for_storage_accounts()
        assert any("STORAGE_BLOB_DATA_CONTRIBUTOR" in message for message in caplog.messages)

    w.cluster_policies.get.assert_not_called()
    w.secrets.get_secret.assert_not_called()
    w.secrets.create_scope.assert_not_called()
