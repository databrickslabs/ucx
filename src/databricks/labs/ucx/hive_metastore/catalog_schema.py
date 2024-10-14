import collections
import logging
from dataclasses import replace
from pathlib import PurePath

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL, Grant, GrantsCrawler
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk.errors.platform import BadRequest

from databricks.labs.ucx.hive_metastore.mapping import TableMapping

logger = logging.getLogger(__name__)


class CatalogSchema:

    def __init__(
        self,
        ws: WorkspaceClient,
        table_mapping: TableMapping,
        principal_grants: PrincipalACL,
        sql_backend: SqlBackend,
        grants_crawler: GrantsCrawler,
        ucx_catalog: str,
    ):
        self._ws = ws
        self._table_mapping = table_mapping
        self._external_locations = list(self._ws.external_locations.list())
        self._principal_grants = principal_grants
        self._backend = sql_backend
        self._hive_grants_crawler = grants_crawler
        self._ucx_catalog = ucx_catalog

    def create_ucx_catalog(self, prompts: Prompts, *, properties: dict[str, str] | None = None) -> None:
        """Create the UCX catalog.

        Args:
            prompts : Prompts
                The prompts object to use for interactive input.
            properties : (dict[str, str] | None), default None
                The properties to pass to the catalog. If None, no properties are passed.
        """
        self._create_catalog_validate(self._ucx_catalog, prompts, properties=properties)

    def create_all_catalogs_schemas(self, prompts: Prompts) -> None:
        candidate_catalogs, candidate_schemas = self._get_missing_catalogs_schemas()
        for candidate_catalog in candidate_catalogs:
            self._create_catalog_validate(candidate_catalog, prompts, properties=None)
        for candidate_catalog, schemas in candidate_schemas.items():
            for candidate_schema in schemas:
                try:
                    self._create_schema(candidate_catalog, candidate_schema)
                except BadRequest as e:
                    if "already exists" in str(e):
                        logger.warning(f"Skipping already existing schema: {candidate_catalog}.{candidate_schema}")
                        continue
        self._apply_from_legacy_table_acls()
        self._update_principal_acl()

    def _apply_from_legacy_table_acls(self) -> None:
        grants = self._get_catalog_schema_hive_grants()
        for grant in grants:
            acl_migrate_sql = grant.uc_grant_sql()
            if acl_migrate_sql is None:
                logger.warning(
                    f"Skipping legacy grant that is not supported in UC: {grant.action_type} on {grant.this_type_and_key()}"
                )
                continue
            logger.debug(f"Migrating acls on {grant.this_type_and_key()} using SQL query: {acl_migrate_sql}")
            self._backend.execute(acl_migrate_sql)

    def _update_principal_acl(self) -> None:
        grants = self._get_catalog_schema_principal_acl_grants()
        for grant in grants:
            acl_migrate_sql = grant.uc_grant_sql()
            if acl_migrate_sql is None:
                logger.warning(
                    f"Skipping legacy grant that is not supported in UC: {grant.action_type} on {grant.this_type_and_key()}"
                )
                continue
            logger.debug(f"Migrating acls on {grant.this_type_and_key()} using SQL query: {acl_migrate_sql}")
            self._backend.execute(acl_migrate_sql)

    def _get_catalog_schema_hive_grants(self) -> list[Grant]:
        src_dst_schema_mapping = self._get_database_source_target_mapping()
        hive_grants = self._hive_grants_crawler.snapshot()
        new_grants: list[Grant] = []
        for grant in hive_grants:
            if grant.this_type_and_key()[0] == "DATABASE" and grant.database:
                for schema in src_dst_schema_mapping[grant.database]:
                    new_grants.append(replace(grant, catalog=schema.catalog_name, database=schema.name))
        catalog_grants: set[Grant] = set()
        for grant in new_grants:
            catalog_grants.add(replace(grant, database=None))
        new_grants.extend(catalog_grants)
        return new_grants

    def _get_catalog_schema_principal_acl_grants(self) -> list[Grant]:
        src_trg_schema_mapping = self._get_database_source_target_mapping()
        grants = self._principal_grants.get_interactive_cluster_grants()
        # filter on grants to only get database level grants
        new_grants: list[Grant] = []
        for grant in grants:
            # For a database grant the table/view are not set, while the database is.
            if grant.table is None and grant.view is None:
                database = grant.database
                if database is not None:
                    new_grants.extend(
                        replace(grant, catalog=schema.catalog_name, database=schema.name)
                        for schema in src_trg_schema_mapping[database]
                    )
        catalog_grants: set[Grant] = set()
        for grant in new_grants:
            catalog_grants.add(replace(grant, database=None))
        new_grants.extend(catalog_grants)
        return new_grants

    def _get_database_source_target_mapping(self) -> dict[str, list[SchemaInfo]]:
        """Generate a dictionary of source database in hive_metastore and its
        mapping of target UC catalog and schema combinations from the table mappings."""
        src_trg_schema_mapping: dict[str, list[SchemaInfo]] = collections.defaultdict(list)
        table_mappings = self._table_mapping.load()
        for table_mapping in table_mappings:
            schema = SchemaInfo(catalog_name=table_mapping.catalog_name, name=table_mapping.dst_schema)
            if schema not in src_trg_schema_mapping[table_mapping.src_schema]:
                src_trg_schema_mapping[table_mapping.src_schema].append(schema)
        return src_trg_schema_mapping

    def _create_catalog_validate(
        self, catalog_name: str, prompts: Prompts, *, properties: dict[str, str] | None
    ) -> None:
        try:
            catalog = self._ws.catalogs.get(catalog_name)
        except NotFound:
            catalog = None
        if catalog:
            logger.warning(f"Skipping already existing catalog: {catalog_name}")
            return
        logger.info(f"Validating UC catalog: {catalog_name}")
        attempts = 3
        while True:
            catalog_storage = prompts.question(
                f"Please provide storage location url for catalog: {catalog_name}", default="metastore"
            )
            if self._validate_location(catalog_storage):
                break
            attempts -= 1
            if attempts == 0:
                raise NotFound(f"Failed to validate location for catalog: {catalog_name}")
        self._create_catalog(catalog_name, catalog_storage, properties=properties)

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

    def _validate_location(self, location: str) -> bool:
        if location == "metastore":
            return True
        try:
            PurePath(location)
        except ValueError:
            logger.error(f"Invalid location path: {location}")
            return False
        for external_location in self._external_locations:
            if external_location.url is not None and location.startswith(external_location.url):
                return True
        logger.warning(f"No matching external location found for: {location}")
        return False

    def _create_catalog(self, catalog: str, catalog_storage: str, *, properties: dict[str, str] | None) -> None:
        logger.info(f"Creating UC catalog: {catalog}")
        if catalog_storage == "metastore":
            self._ws.catalogs.create(catalog, comment="Created by UCX", properties=properties)
        else:
            self._ws.catalogs.create(
                catalog,
                storage_root=catalog_storage,
                comment="Created by UCX",
                properties=properties,
            )

    def _create_schema(self, catalog, schema):
        logger.info(f"Creating UC schema: {schema} in catalog: {catalog}")
        self._ws.schemas.create(schema, catalog, comment="Created by UCX")
