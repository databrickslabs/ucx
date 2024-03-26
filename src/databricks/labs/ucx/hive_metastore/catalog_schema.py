import logging
from pathlib import PurePath

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.mapping import TableMapping

logger = logging.getLogger(__name__)


class CatalogSchema:
    def __init__(self, ws: WorkspaceClient, table_mapping: TableMapping):
        self._ws = ws
        self._table_mapping = table_mapping
        self._external_locations = self._ws.external_locations.list()

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, installation: Installation):
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        table_mapping = TableMapping(installation, ws, sql_backend)
        return cls(ws, table_mapping)

    def create_all_catalogs_schemas(self, prompts: Prompts):
        candidate_catalogs, candidate_schemas = self._get_missing_catalogs_schemas()
        for candidate_catalog in candidate_catalogs:
            self._create_catalog_validate(candidate_catalog, prompts)
        for candidate_catalog, schemas in candidate_schemas.items():
            for candidate_schema in schemas:
                self._create_schema(candidate_catalog, candidate_schema)

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
