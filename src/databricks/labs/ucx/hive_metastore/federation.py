import collections
import logging
from enum import Enum

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, NotFound, BadRequest
from databricks.sdk.service.catalog import (
    ConnectionInfo,
    SecurableType,
    Privilege,
    PermissionsChange,
)

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore import ExternalLocations


logger = logging.getLogger(__name__)

class ConnectionType(Enum):
    """The type of connection."""

    BIGQUERY = 'BIGQUERY'
    DATABRICKS = 'DATABRICKS'
    MYSQL = 'MYSQL'
    POSTGRESQL = 'POSTGRESQL'
    REDSHIFT = 'REDSHIFT'
    SNOWFLAKE = 'SNOWFLAKE'
    SQLDW = 'SQLDW'
    SQLSERVER = 'SQLSERVER'
    HIVE_METASTORE = 'HIVE_METASTORE'


class HiveMetastoreFederation:
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        external_locations: ExternalLocations,
        workspace_info: WorkspaceInfo,
    ):
        self._workspace_client = workspace_client
        self._external_locations = external_locations
        self._workspace_info = workspace_info

    def run(self):
        name = self._workspace_info.current()
        connection_info = self._get_or_create_connection(name)
        try:
            self._workspace_client.catalogs.create(
                name=connection_info.name,
                connection_name=connection_info.name,
                options={"authorized_paths": self._get_authorized_paths()},
            )
        except BadRequest as err:
            if err.error_code == 'CATALOG_ALREADY_EXISTS':
                logger.info(f'Catalog {connection_info.name} already exists')
                for catalog_info in self._workspace_client.catalogs.list():
                    if catalog_info.name == connection_info.name:
                        return catalog_info
            raise err

    def _get_or_create_connection(self, name: str) -> ConnectionInfo:
        try:
            return self._workspace_client.connections.create(
                name=name,
                connection_type=ConnectionType.HIVE_METASTORE,  # needs SDK change
                options={"builtin": "true"},
            )
        except AlreadyExists:
            for connection in self._workspace_client.connections.list():
                if connection.name == name:
                    return connection
        raise NotFound(f'Connection {name} not found')

    def _get_authorized_paths(self) -> str:
        existing = {}
        for external_location in self._workspace_client.external_locations.list():
            existing[external_location.url] = external_location
        authorized_paths = []
        current_user = self._workspace_client.current_user.me()
        if not current_user.user_name:
            raise ValueError('Current user not found')
        for external_location_info in self._external_locations.snapshot():
            location = external_location_info.location.rstrip('/').replace('s3a://', 's3://')
            existing_location = existing.get(location)
            if not existing_location:
                logger.warning(f'External location {location} not found')
                continue
            location_name = existing_location.name
            if not location_name:
                logger.warning(f'External location {location} has no name')
                continue
            self._add_missing_permissions_if_needed(location_name, current_user.user_name)
            authorized_paths.append(location)
        return ",".join(authorized_paths)

    def _add_missing_permissions_if_needed(self, location_name: str, current_user: str):
        grants = self._location_grants(location_name)
        if Privilege.CREATE_FOREIGN_CATALOG not in grants[current_user]:
            change = PermissionsChange(principal=current_user, add=[Privilege.CREATE_FOREIGN_CATALOG])
            self._workspace_client.grants.update(SecurableType.EXTERNAL_LOCATION, location_name, changes=[change])

    def _location_grants(self, location_name: str) -> dict[str, set[Privilege]]:
        grants: dict[str, set[Privilege]] = collections.defaultdict(set)
        result = self._workspace_client.grants.get(SecurableType.EXTERNAL_LOCATION, location_name)
        if not result.privilege_assignments:
            return grants
        for assignment in result.privilege_assignments:
            if not assignment.privileges:
                continue
            if not assignment.principal:
                continue
            for privilege in assignment.privileges:
                grants[assignment.principal].add(privilege)
        return grants
