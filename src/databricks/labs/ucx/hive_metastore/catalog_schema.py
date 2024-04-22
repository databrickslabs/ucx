import logging
from pathlib import PurePath
import dataclasses

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL, Grant
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore.mapping import TableMapping

logger = logging.getLogger(__name__)


class CatalogSchema:
    def __init__(
        self, ws: WorkspaceClient, table_mapping: TableMapping, principal_grants: PrincipalACL, sql_backend: SqlBackend
    ):
        self._ws = ws
        self._table_mapping = table_mapping
        self._external_locations = self._ws.external_locations.list()
        self._principal_grants = principal_grants
        self._backend = sql_backend

    def create_all_catalogs_schemas(self, prompts: Prompts):
        candidate_catalogs, candidate_schemas = self._get_missing_catalogs_schemas()
        for candidate_catalog in candidate_catalogs:
            self._create_catalog_validate(candidate_catalog, prompts)
        for candidate_catalog, schemas in candidate_schemas.items():
            for candidate_schema in schemas:
                self._create_schema(candidate_catalog, candidate_schema)
        self._update_principal_acl()

    def _update_principal_acl(self):
        grants = self._get_catalog_schema_grants()
        for grant in grants:
            acl_migrate_sql = grant.uc_grant_sql()
            if acl_migrate_sql is None:
                logger.warning(f"Cannot identify UC grant for {grant.this_type_and_key()}. Skipping.")
                continue
            logger.debug(f"Migrating acls on {grant.this_type_and_key()} using SQL query: {acl_migrate_sql}")
            self._backend.execute(acl_migrate_sql)

    def _get_catalog_schema_grants(self):
        catalog_grants: set[Grant] = set()
        new_grants = []
        src_trg_schema_mapping = self._get_database_source_target_mapping()
        grants = self._principal_grants.get_interactive_cluster_grants()
        # filter on grants to only get database level grants
        database_grants = [grant for grant in grants if grant.table is None and grant.view is None]
        for db_grant in database_grants:
            new_grants.append(
                dataclasses.replace(
                    db_grant,
                    # replace source database with taget UC database
                    database=src_trg_schema_mapping[db_grant.database]['target_schema'],
                    # replace hive_metastore with target UC catalog
                    catalog=src_trg_schema_mapping[db_grant.database]['target_catalog'],
                )
            )
        for grant in new_grants:
            catalog_grants.add(dataclasses.replace(grant, database=None))
        new_grants.extend(catalog_grants)
        return new_grants

    def _get_database_source_target_mapping(self) -> dict[str, dict]:
        """generate a dictionary of source database in hive_metastore and its
        mapping of target UC catalog and schema from the table mappings."""
        src_trg_schema_mapping: dict[str, dict] = {}
        table_mappings = self._table_mapping.load()
        for mappings in table_mappings:
            if mappings.src_schema not in src_trg_schema_mapping:
                src_trg_schema_mapping[mappings.src_schema] = {
                    'target_catalog': mappings.catalog_name,
                    'target_schema': mappings.dst_schema,
                }
                continue
        return src_trg_schema_mapping

    def _create_catalog_validate(self, catalog, prompts: Prompts):
        logger.info(f"Creating UC catalog: {catalog}")
        # create catalogs
        attempts = 3
        while True:
            catalog_storage = prompts.question(
                f"Please provide storage location url for catalog:{catalog}.", default="metastore"
            )
            if self._validate_location(catalog_storage):
                break
            attempts -= 1
            if attempts == 0:
                raise NotFound(f"Failed to validate location for {catalog} catalog")
        self._create_catalog(catalog, catalog_storage)

    def _list_existing(self) -> tuple[set[str], dict[str, set[str]]]:
        """generate a list of existing UC catalogs and schema."""
        logger.info("Listing existing UC catalogs and schemas")
        existing_catalogs: set[str] = set()
        for catalog_info in self._ws.catalogs.list():
            if catalog_info.name:
                existing_catalogs.add(catalog_info.name)

        existing_schemas: dict[str, set[str]] = {}  # catalog -> set[schema]
        for catalog in existing_catalogs:
            existing_schemas[catalog] = set()
            for schema in self._ws.schemas.list(catalog, max_results=0):
                if schema.name:
                    existing_schemas[catalog].add(schema.name)

        return existing_catalogs, existing_schemas

    def _list_target(self) -> tuple[set[str], dict[str, set[str]]]:
        """generate a list of catalogs and schema to be created from table mappings."""
        target_catalogs: set[str] = set()
        target_schemas: dict[str, set[str]] = {}  # catalog -> set[schema]
        table_mappings = self._table_mapping.load()
        for mappings in table_mappings:
            target_catalog = mappings.catalog_name
            target_schema = mappings.dst_schema
            target_catalogs.add(target_catalog)
            if target_catalog not in target_schemas:
                target_schemas[target_catalog] = {target_schema}
                continue
            target_schemas[target_catalog].add(target_schema)
        return target_catalogs, target_schemas

    def _get_missing_catalogs_schemas(self) -> tuple[set[str], dict[str, set[str]]]:
        """prepare a list of catalogs and schema to be created"""
        existing_catalogs, existing_schemas = self._list_existing()
        target_catalogs, target_schemas = self._list_target()

        logger.info("Preparing a list of UC catalogs and schema to be created")
        # filter out existing catalogs and schemas from target catalogs and schemas to be created.
        for existing_catalog in existing_catalogs:
            if existing_catalog in target_catalogs:
                target_catalogs.remove(existing_catalog)

        for catalog, schemas in existing_schemas.items():
            if catalog in target_schemas:
                target_schemas[catalog] = target_schemas[catalog] - schemas
        return target_catalogs, target_schemas

    def _validate_location(self, location: str):
        if location == "metastore":
            return True
        try:
            location_path = PurePath(location)
        except ValueError:
            logger.error(f"Invalid location path {location}")
            return False
        for external_location in self._external_locations:
            if location == external_location.url:
                return True
            if location_path.match(f"{external_location.url}/*"):
                return True
        return False

    def _create_catalog(self, catalog, catalog_storage):
        logger.info(f"Creating UC catalog: {catalog}")
        if catalog_storage == "metastore":
            self._ws.catalogs.create(catalog, comment="Created by UCX")
        else:
            self._ws.catalogs.create(catalog, storage_root=catalog_storage, comment="Created by UCX")

    def _create_schema(self, catalog, schema):
        logger.info(f"Creating UC schema: {schema} in catalog: {catalog}")
        self._ws.schemas.create(schema, catalog, comment="Created by UCX")
