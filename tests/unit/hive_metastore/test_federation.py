import base64
from unittest.mock import create_autospec, call

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.catalog import (
    ConnectionType,
    CatalogInfo,
    ExternalLocationInfo,
    PermissionsList,
    PrivilegeAssignment,
    SecurableType,
    PermissionsChange,
    ConnectionInfo,
)
from databricks.sdk.service.iam import User
from databricks.sdk.service.workspace import GetSecretResponse

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.federation import (
    HiveMetastoreFederation,
    HiveMetastoreFederationEnabler,
    Privilege,
)
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation


def test_create_federated_catalog_int(mock_installation):
    workspace_client = create_autospec(WorkspaceClient)
    external_locations = create_autospec(ExternalLocations)
    workspace_info = create_autospec(WorkspaceInfo)

    workspace_info.current.return_value = 'a'
    external_locations.external_locations_with_root.return_value = [
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

    hms_fed = HiveMetastoreFederation(
        workspace_client,
        external_locations,
        workspace_info,
        mock_installation.load(WorkspaceConfig),
        enable_hms_federation=True,
    )

    hms_fed.create_from_cli(MockPrompts({".*": ""}))

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
            changes=[PermissionsChange(principal='serge', add=[Privilege.CREATE_FOREIGN_SECURABLE])],
        ),
        call.get(SecurableType.EXTERNAL_LOCATION, 'e'),
        call.update(
            SecurableType.EXTERNAL_LOCATION,
            'e',
            changes=[PermissionsChange(principal='serge', add=[Privilege.CREATE_FOREIGN_SECURABLE])],
        ),
    ]
    assert calls == workspace_client.grants.method_calls


def test_create_federated_catalog_ext(mock_installation):
    workspace_client = create_autospec(WorkspaceClient)
    external_locations = create_autospec(ExternalLocations)
    workspace_info = create_autospec(WorkspaceInfo)

    workspace_info.current.return_value = 'a'
    external_locations.external_locations_with_root.return_value = [
        ExternalLocation('s3://b/c/d', 1),
    ]
    workspace_client.current_user.me.return_value = User(user_name='serge')
    workspace_client.connections.create.return_value = CatalogInfo(name='a')
    workspace_client.secrets.get_secret.return_value = GetSecretResponse(
        key='secret_key', value=base64.standard_b64encode('bar'.encode()).decode()
    )
    workspace_client.external_locations.list.return_value = [
        ExternalLocationInfo(url='s3://b/c/d', name='b'),
    ]
    workspace_client.grants.get.return_value = PermissionsList(
        privilege_assignments=[PrivilegeAssignment(privileges=[Privilege.MANAGE], principal='any')]
    )
    mock_installation.load = lambda _: WorkspaceConfig(
        inventory_database='ucx',
        spark_conf={
            "spark.hadoop.javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
            "spark.hadoop.javax.jdo.option.ConnectionPassword": "{{secrets/secret_scope/secret_key}}",
            "spark.hadoop.javax.jdo.option.ConnectionURL": "jdbc:mysql://hostname.us-east-2.rds.amazonaws.com:3306/metastore",
            "spark.hadoop.javax.jdo.option.ConnectionUserName": "foo",
            "spark.sql.hive.metastore.jars": "maven",
            "spark.sql.hive.metastore.version": "2.3.0",
        },
    )

    hms_fed = HiveMetastoreFederation(
        workspace_client,
        external_locations,
        workspace_info,
        mock_installation.load(WorkspaceConfig),
        enable_hms_federation=True,
    )

    hms_fed.create_from_cli(
        MockPrompts({"A supported external Hive Metastore.*": "yes", "Enter the name*": "fed_source"})
    )

    workspace_client.connections.create.assert_called_with(
        name='fed_source',
        connection_type=ConnectionType.HIVE_METASTORE,
        options={
            'database': 'metastore',
            'db_type': 'mysql',
            'host': 'hostname.us-east-2.rds.amazonaws.com',
            'password': 'bar',
            'port': '3306',
            'user': 'foo',
            'version': '2.3',
        },
    )
    workspace_client.catalogs.create.assert_called_with(
        name='a',
        connection_name='a',
        options={"authorized_paths": 's3://b/c/d'},
    )
    calls = [
        call.get(SecurableType.EXTERNAL_LOCATION, 'b'),
        call.update(
            SecurableType.EXTERNAL_LOCATION,
            'b',
            changes=[PermissionsChange(principal='serge', add=[Privilege.CREATE_FOREIGN_SECURABLE])],
        ),
    ]
    assert calls == workspace_client.grants.method_calls


def test_already_existing_connection(mock_installation):
    workspace_client = create_autospec(WorkspaceClient)
    external_locations = create_autospec(ExternalLocations)
    workspace_info = create_autospec(WorkspaceInfo)

    workspace_info.current.return_value = 'a'
    external_locations.external_locations_with_root.return_value = [
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

    hms_fed = HiveMetastoreFederation(
        workspace_client,
        external_locations,
        workspace_info,
        mock_installation.load(WorkspaceConfig),
        enable_hms_federation=True,
    )

    prompts = MockPrompts({".*": ""})

    hms_fed.create_from_cli(prompts)

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
