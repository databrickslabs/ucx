import collections
import logging
import re
from dataclasses import dataclass, replace
from functools import cached_property
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
from databricks.labs.ucx.assessment.aws import AWSGlue
from databricks.labs.ucx.assessment.secrets import SecretsMixin
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations

logger = logging.getLogger(__name__)


@dataclass
class ExternalHmsInfo:
    """
    This is a dataclass that represents the external Hive Metastore connection information.
    It supports non glue external metastores.
    """

    database_type: str
    host: str
    port: str
    database: str
    user: str | None
    password: str | None
    version: str | None

    def as_dict(self) -> dict[str, str]:
        return {
            "database": self.database,
            "db_type": self.database_type,
            "host": self.host,
            "port": self.port,
        }


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
        ws: WorkspaceClient,
        external_locations: ExternalLocations,
        workspace_info: WorkspaceInfo,
        config: WorkspaceConfig,
        *,
        enable_hms_federation: bool = False,
    ):
        self._ws = ws
        self._external_locations = external_locations
        self._workspace_info = workspace_info
        self._enable_hms_federation = enable_hms_federation
        self._config = config

    # Supported databases and version for HMS Federation
    supported_database_versions: ClassVar[dict[str, list[str]]] = {
        "mysql": ["2.3", "0.13"],
    }

    def create_from_cli(self, prompts: Prompts) -> None:
        if not self._enable_hms_federation:
            raise RuntimeWarning('Run `databricks labs ucx enable-hms-federation` to enable HMS Federation')

        name = prompts.question(
            'Enter the name of the Hive Metastore connection and catalog', default=self._workspace_info.current()
        )

        if self._glue_metastore and prompts.confirm(
            'A supported AWS Glue Metastore connection was identified. Use it?'
        ):
            connection_info = self._get_or_create_glue_connection(name, self._glue_metastore)
        elif self._external_hms and prompts.confirm(
            f'A supported external Hive Metastore connection was identified: {self._external_hms.database_type}. '
            f'Use this connection?'
        ):
            connection_info = self._get_or_create_ext_connection(name, self._external_hms)
        else:
            connection_info = self._get_or_create_int_connection(name)

        assert connection_info.name is not None
        self._register_federated_catalog(connection_info)

    @cached_property
    def _external_hms(self) -> ExternalHmsInfo | None:
        if not self._config.spark_conf:
            logger.info('Spark config not found')
            return None
        spark_config = self._config.spark_conf
        jdbc_url = self._get_value_from_config_key(spark_config, 'spark.hadoop.javax.jdo.option.ConnectionURL')
        if not jdbc_url:
            logger.info('JDBC URL not found')
            return None
        version = self._get_value_from_config_key(spark_config, 'spark.sql.hive.metastore.version')
        if not version:
            logger.info('Hive Metastore version not found')
            return None
        major_minor_match = re.match(r'(^\d+\.\d+)', version)
        if not major_minor_match:
            logger.info(f'Wrong Hive Metastore Database Version Format: {version}')
            return None
        major_minor_version = major_minor_match.group(1)
        external_hms = replace(self._split_jdbc_url(jdbc_url), version=major_minor_version)
        supported_versions = self.supported_database_versions.get(external_hms.database_type)
        if not supported_versions:
            logger.info(f'Unsupported Hive Metastore: {external_hms.database_type}')
            return None
        if major_minor_version not in supported_versions:
            logger.info(f'Unsupported Hive Metastore Version: {external_hms.database_type} - {version}')
            return None

        if not external_hms.user:
            external_hms = replace(
                external_hms,
                user=self._get_value_from_config_key(spark_config, 'spark.hadoop.javax.jdo.option.ConnectionUserName'),
            )
        if not external_hms.password:
            external_hms = replace(
                external_hms,
                password=self._get_value_from_config_key(
                    spark_config, 'spark.hadoop.javax.jdo.option.ConnectionPassword'
                ),
            )
        return external_hms

    @cached_property
    def _glue_metastore(self) -> AWSGlue | None:
        if not self._ws.config.is_aws:
            return None
        if not self._config.spark_conf:
            logger.info('Spark config not found')
            return None
        spark_config = self._config.spark_conf
        glue_metastore = spark_config.get('spark.databricks.hive.metastore.glueCatalog.enabled')
        if not glue_metastore or glue_metastore.lower() != 'true':
            logger.info('Glue Metastore not enabled')
            return None
        instance_profile = self._config.instance_profile
        if not instance_profile:
            logger.info('Instance Profile not found')
            return None
        return AWSGlue.get_glue_for_workspace(self._ws, instance_profile)

    @classmethod
    def _split_jdbc_url(cls, jdbc_url: str) -> ExternalHmsInfo:
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

        return ExternalHmsInfo(db_type, host, port, database, user, password, None)

    def _register_federated_catalog(
        self,
        connection_info,
    ) -> CatalogInfo:
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
            return self._get_existing_connection(name)

    def _get_existing_connection(self, name: str) -> ConnectionInfo:
        for connection in self._ws.connections.list():
            if connection.name == name:
                return connection
        raise NotFound(f'Connection {name} not found')

    def _get_or_create_ext_connection(self, name: str, external_hms: ExternalHmsInfo) -> ConnectionInfo:
        options = external_hms.as_dict()
        if external_hms.user:
            options["user"] = external_hms.user
        if external_hms.password:
            options["password"] = external_hms.password
        if external_hms.version:
            options["version"] = external_hms.version
        try:
            return self._ws.connections.create(
                name=name,
                connection_type=ConnectionType.HIVE_METASTORE,  # needs SDK change
                options=options,
            )
        except AlreadyExists:
            return self._get_existing_connection(name)

    def _get_or_create_glue_connection(self, name: str, aws_glue: AWSGlue) -> ConnectionInfo:
        options = aws_glue.as_dict()
        try:
            return self._ws.connections.create(
                name=name,
                connection_type=ConnectionType.HIVE_METASTORE,  # needs SDK change
                options=options,
            )
        except AlreadyExists:
            return self._get_existing_connection(name)

    def _get_authorized_paths(self) -> str:
        existing = {}
        for external_location in self._ws.external_locations.list():
            existing[external_location.url] = external_location
        authorized_paths = []
        current_user = self._ws.current_user.me()
        if not current_user.user_name:
            raise NotFound('Current user not found')
        # Get the external locations. If not using external HMS, include the root DBFS location.
        if self._external_hms is not None:
            external_locations = self._external_locations.external_locations_with_root()
        else:
            external_locations = self._external_locations.snapshot()

        for external_location_info in external_locations:
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
