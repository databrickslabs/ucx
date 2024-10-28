from unittest.mock import create_autospec, call

from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.catalog import (
    ConnectionType,
    CatalogInfo,
    ExternalLocationInfo,
    PermissionsList,
    PrivilegeAssignment,
    Privilege,
    SecurableType,
    PermissionsChange,
    ConnectionInfo,
)
from databricks.sdk.service.iam import User

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.federation import HiveMetastoreFederation, HiveMetastoreFederationEnabler
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation


def test_create_federated_catalog():
    workspace_client = create_autospec(WorkspaceClient)
    external_locations = create_autospec(ExternalLocations)
    workspace_info = create_autospec(WorkspaceInfo)

    workspace_info.current.return_value = 'a'
    external_locations.snapshot.return_value = [
        ExternalLocation('s3://b/c/d', 1),
        ExternalLocation('s3://e/f/g', 1),
        ExternalLocation('s3://h/i/j', 1),
    ]
    workspace_client.current_user.me.return_value = User(user_name='serge')
    workspace_client.connections.create.return_value = CatalogInfo(name='a')
    workspace_client.external_locations.list.return_value = [
        ExternalLocationInfo(url='s3://b/c/d', name='b'),
        ExternalLocationInfo(url='s3://e/f/g', name='e'),
    ]
    workspace_client.grants.get.return_value = PermissionsList(
        privilege_assignments=[PrivilegeAssignment(privileges=[Privilege.MANAGE], principal='any')]
    )

    hms_fed = HiveMetastoreFederation(workspace_client, external_locations, workspace_info, enable_hms_federation=True)
    hms_fed.register_hms_as_federated_catalog()

    workspace_client.connections.create.assert_called_with(
        name='a',
        connection_type=ConnectionType.HIVE_METASTORE,
        options={"builtin": "true"},
    )
    workspace_client.catalogs.create.assert_called_with(
        name='a',
        connection_name='a',
        options={"authorized_paths": 's3://b/c/d,s3://e/f/g'},
    )
    calls = [
        call.get(SecurableType.EXTERNAL_LOCATION, 'b'),
        call.update(
            SecurableType.EXTERNAL_LOCATION,
            'b',
            changes=[PermissionsChange(principal='serge', add=[Privilege.CREATE_FOREIGN_CATALOG])],
        ),
        call.get(SecurableType.EXTERNAL_LOCATION, 'e'),
        call.update(
            SecurableType.EXTERNAL_LOCATION,
            'e',
            changes=[PermissionsChange(principal='serge', add=[Privilege.CREATE_FOREIGN_CATALOG])],
        ),
    ]
    assert calls == workspace_client.grants.method_calls


def test_already_existing_connection():
    workspace_client = create_autospec(WorkspaceClient)
    external_locations = create_autospec(ExternalLocations)
    workspace_info = create_autospec(WorkspaceInfo)

    workspace_info.current.return_value = 'a'
    external_locations.snapshot.return_value = [
        ExternalLocation('s3://b/c/d', 1),
        ExternalLocation('s3://e/f/g', 1),
        ExternalLocation('s3://h/i/j', 1),
    ]
    workspace_client.current_user.me.return_value = User(user_name='serge')
    workspace_client.connections.create.side_effect = AlreadyExists(...)
    workspace_client.connections.list.return_value = [ConnectionInfo(name='a'), ConnectionInfo(name='b')]
    workspace_client.external_locations.list.return_value = [
        ExternalLocationInfo(url='s3://b/c/d', name='b'),
        ExternalLocationInfo(url='s3://e/f/g', name='e'),
    ]
    workspace_client.grants.get.return_value = PermissionsList(
        privilege_assignments=[PrivilegeAssignment(privileges=[Privilege.MANAGE], principal='any')]
    )

    hms_fed = HiveMetastoreFederation(workspace_client, external_locations, workspace_info, enable_hms_federation=True)
    hms_fed.register_hms_as_federated_catalog()

    workspace_client.connections.create.assert_called_with(
        name='a',
        connection_type=ConnectionType.HIVE_METASTORE,
        options={"builtin": "true"},
    )
    workspace_client.catalogs.create.assert_called_with(
        name='a',
        connection_name='a',
        options={"authorized_paths": 's3://b/c/d,s3://e/f/g'},
    )


def test_hms_federation_enabler():
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx',
                'connect': {
                    'host': 'host',
                    'token': 'token',
                },
            }
        }
    )
    hmse = HiveMetastoreFederationEnabler(installation)
    hmse.enable()

    config = installation.load(WorkspaceConfig)
    assert config.enable_hms_federation is True
