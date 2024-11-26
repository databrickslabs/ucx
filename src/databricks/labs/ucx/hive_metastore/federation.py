import collections
import logging
import re
from dataclasses import dataclass
from typing import ClassVar

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, NotFound, BadRequest
from databricks.sdk.service.catalog import (
    ConnectionType,
    ConnectionInfo,
    SecurableType,
    PermissionsChange,
    CatalogInfo,
    Privilege,
)

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.assessment.secrets import SecretsMixin
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.hive_metastore import ExternalLocations


logger = logging.getLogger(__name__)


@dataclass
class ExtHms:
    # This is a dataclass that represents the external Hive Metastore connection information
    database: str
    db_type: str
    host: str
    password: str
    port: int
    user: str
    version: str


class HiveMetastoreFederationEnabler:
    def __init__(self, installation: Installation):
        self._installation = installation

    def enable(self):
        config = self._installation.load(WorkspaceConfig)
        config.enable_hms_federation = True
        self._installation.save(config)


class HiveMetastoreFederation(SecretsMixin):
    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        external_locations: ExternalLocations,
        workspace_info: WorkspaceInfo,
        enable_hms_federation: bool = False,
    ):
        self._ws = ws
        self._external_locations = external_locations
        self._workspace_info = workspace_info
        self._enable_hms_federation = enable_hms_federation
        self._installation = installation

    supported_db_vers: ClassVar[dict[str, list[str]]] = {
        "mysql": ["2.3.0", "0.13"],
    }

    def create_from_cli(self, prompts: Prompts):


    def _get_ext_hms(self) -> ExtHms:
        config = self._installation.load(WorkspaceConfig)
        if not config.spark_config:
            raise ValueError('Spark config not found')
        spark_config = config.spark_config
        jdbc_url = self._get_value_from_config_key(spark_config,'spark.datasource.hive.metastore.jdbc.url')

    @classmethod
    def _split_jdbc_url(cls, jdbc_url: str) -> tuple[str, str, int, str, str, str]:
        # Define the regex pattern to match the JDBC URL components
        pattern = re.compile(
            r'jdbc:(?P<db_type>[a-zA-Z0-9]+)://(?P<host>[^:/]+):(?P<port>\d+)/(?P<database>[^?]+)(\?user=(?P<user>[^&]+)&password=(?P<password>[^&]+))?'
        )
        match = pattern.match(jdbc_url)
        if not match:
            raise ValueError(f'Unsupported JDBC URL: {jdbc_url}')

        db_type = match.group('db_type')
        host = match.group('host')
        port = int(match.group('port'))
        database = match.group('database')
        user = match.group('user')
        password = match.group('password')

        return db_type, host, port, database, user, password


    def register_internal_hms_as_federated_catalog(self) -> CatalogInfo:
        if not self._enable_hms_federation:
            raise RuntimeWarning('Run `databricks labs ucx enable-hms-federation` to enable HMS Federation')
        name = self._workspace_info.current()
        connection_info = self._get_or_create_int_connection(name)
        assert connection_info.name is not None
        try:
            return self._ws.catalogs.create(
                name=connection_info.name,
                connection_name=connection_info.name,
                options={"authorized_paths": self._get_authorized_paths()},
            )
        except BadRequest as err:
            if err.error_code == 'CATALOG_ALREADY_EXISTS':
                logger.info(f'Catalog {connection_info.name} already exists')
                for catalog_info in self._ws.catalogs.list():
                    if catalog_info.name == connection_info.name:
                        return catalog_info
            raise err

    def _get_or_create_int_connection(self, name: str) -> ConnectionInfo:
        try:
            return self._ws.connections.create(
                name=name,
                connection_type=ConnectionType.HIVE_METASTORE,  # needs SDK change
                options={"builtin": "true"},
            )
        except AlreadyExists:
            for connection in self._ws.connections.list():
                if connection.name == name:
                    return connection
        raise NotFound(f'Connection {name} not found')

    def _get_or_create_ext_connection(self, name:str, ext_hms: ExtHms) -> ConnectionInfo:
        try:
            return self._ws.connections.create(
                name=name,
                connection_type=ConnectionType.HIVE_METASTORE,  # needs SDK change
                options={
                    "builtin": "true",
                    "database": ext_hms.database,
                    "db_type": ext_hms.db_type,
                    "host": ext_hms.host,
                    "password": ext_hms.password,
                    "port": ext_hms.port,
                    "user": ext_hms.user,
                    "version": ext_hms.version,
                },
            )
        except AlreadyExists:
            for connection in self._ws.connections.list():
                if connection.name == name:
                    return connection
        raise NotFound(f'Connection {name} not found')

    def _get_authorized_paths(self) -> str:
        existing = {}
        for external_location in self._ws.external_locations.list():
            existing[external_location.url] = external_location
        authorized_paths = []
        current_user = self._ws.current_user.me()
        if not current_user.user_name:
            raise NotFound('Current user not found')
        for external_location_info in self._external_locations.external_locations_with_root():
            location = ExternalLocations.clean_location(external_location_info.location)
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
        if Privilege.CREATE_FOREIGN_SECURABLE not in grants[current_user]:
            change = PermissionsChange(principal=current_user, add=[Privilege.CREATE_FOREIGN_SECURABLE])
            self._ws.grants.update(SecurableType.EXTERNAL_LOCATION, location_name, changes=[change])

    def _location_grants(self, location_name: str) -> dict[str, set[Privilege]]:
        grants: dict[str, set[Privilege]] = collections.defaultdict(set)
        result = self._ws.grants.get(SecurableType.EXTERNAL_LOCATION, location_name)
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
