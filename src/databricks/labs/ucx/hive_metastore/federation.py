import collections
import logging
import re
from dataclasses import dataclass, replace
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
from databricks.labs.ucx.hive_metastore import ExternalLocations


logger = logging.getLogger(__name__)


@dataclass
class ExtHms:
    # This is a dataclass that represents the external Hive Metastore connection information
    db_type: str
    host: str
    port: str
    database: str
    user: str | None
    password: str | None
    version: str | None


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
        if not self._enable_hms_federation:
            raise RuntimeWarning('Run `databricks labs ucx enable-hms-federation` to enable HMS Federation')
        name = self._workspace_info.current()

        ext_hms = None
        try:
            ext_hms = self._get_ext_hms()
        except ValueError:
            logger.info('Failed to get external Hive Metastore connection information')

        if ext_hms and prompts.confirm(
            f'A supported external Hive Metastore connection was identified: {ext_hms.db_type}. Use this connection?'
        ):
            connection_info = self._get_or_create_ext_connection(name, ext_hms)
        else:
            connection_info = self._get_or_create_int_connection(name)
        assert connection_info.name is not None
        return self._register_federated_catalog(connection_info)

    def _get_ext_hms(self) -> ExtHms:
        config = self._installation.load(WorkspaceConfig)
        if not config.spark_conf:
            raise ValueError('Spark config not found')
        spark_config = config.spark_conf
        jdbc_url = self._get_value_from_config_key(spark_config, 'spark.hadoop.javax.jdo.option.ConnectionURL')
        if not jdbc_url:
            raise ValueError('JDBC URL not found')
        version = self._get_value_from_config_key(spark_config, 'spark.sql.hive.metastore.version')
        if not version:
            raise ValueError('Hive Metastore version not found')
        ext_hms = replace(self._split_jdbc_url(jdbc_url), version=version)
        supported_versions = self.supported_db_vers.get(ext_hms.db_type)
        if not supported_versions:
            raise ValueError(f'Unsupported Hive Metastore: {ext_hms.db_type}')
        if version not in supported_versions:
            raise ValueError(f'Unsupported Hive Metastore Version: {ext_hms.db_type} - {version}')
        if not ext_hms.user:
            ext_hms = replace(
                ext_hms,
                user=self._get_value_from_config_key(spark_config, 'spark.hadoop.javax.jdo.option.ConnectionUserName'),
            )
        if not ext_hms.password:
            ext_hms = replace(
                ext_hms,
                password=self._get_value_from_config_key(
                    spark_config, 'spark.hadoop.javax.jdo.option.ConnectionPassword'
                ),
            )
        return ext_hms

    @classmethod
    def _split_jdbc_url(cls, jdbc_url: str) -> ExtHms:
        # Define the regex pattern to match the JDBC URL components
        pattern = re.compile(
            r'jdbc:(?P<db_type>[a-zA-Z0-9]+)://(?P<host>[^:/]+):(?P<port>\d+)/(?P<database>[^?]+)(\?user=(?P<user>[^&]+)&password=(?P<password>[^&]+))?'
        )
        match = pattern.match(jdbc_url)
        if not match:
            raise ValueError(f'Unsupported JDBC URL: {jdbc_url}')

        db_type = match.group('db_type')
        host = match.group('host')
        port = match.group('port')
        database = match.group('database')
        user = match.group('user')
        password = match.group('password')

        return ExtHms(db_type, host, port, database, user, password, None)

    def _register_federated_catalog(self, connection_info) -> CatalogInfo:
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

    def _get_or_create_ext_connection(self, name: str, ext_hms: ExtHms) -> ConnectionInfo:
        options: dict[str, str] = {
            "builtin": "false",
            "database": ext_hms.database,
            "db_type": ext_hms.db_type,
            "host": ext_hms.host,
            "port": ext_hms.port,
        }
        if ext_hms.user:
            options["user"] = ext_hms.user
        if ext_hms.password:
            options["password"] = ext_hms.password
        if ext_hms.version:
            options["version"] = ext_hms.version
        try:
            return self._ws.connections.create(
                name=name,
                connection_type=ConnectionType.HIVE_METASTORE,  # needs SDK change
                options=options,
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
