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

    def create_all_catalogs_schemas(self, prompts: Prompts) -> None:
        existing_catalogs: set[str] = self._get_existing_catalogs()
        all_catalog_schemas: dict[str, set[str]] = self._get_all_catalog_schemas_to_create()

        for catalog, all_schemas in all_catalog_schemas.items():
            if catalog in existing_catalogs:
                existing_schemas = self._get_schemas_of_catalog(catalog)
                missing_schemas = all_schemas - existing_schemas
                if len(missing_schemas) > 0:
                    self._create_schemas(catalog, missing_schemas)
            else:
                # Create a new catalog with it's belonging schemas.
                self._validate_create_catalog(catalog, prompts)
                self._create_schemas(catalog, all_schemas)

    def _validate_create_catalog(self, catalog, prompts: Prompts) -> None:
        logger.info(f"Creating UC catalog: {catalog}")
        attempts = 3
        while attempts > 0:
            catalog_storage = prompts.question(
                f"Please provide storage location url for catalog:{catalog}.", default="metastore"
            )
            if self._is_valid_location(catalog_storage):
                self._create_catalog(catalog, catalog_storage)
                return
            attempts -= 1
        raise NotFound(f"Failed to validate location for {catalog} catalog")

    def _get_existing_catalogs(self) -> set[str]:
        return {catalog.name for catalog in self._ws.catalogs.list()}

    def _get_schemas_of_catalog(self, catalog: str) -> set[str]:
        return {schema.name for schema in self._ws.schemas.list(catalog, max_results=0) if schema}

    def _get_all_catalog_schemas_to_create(self) -> dict[str, set[str]]:
        catalog_schemas: dict[str, set[str]] = {}
        for table in self._table_mapping.load():
            catalog, schema = table.catalog_name, table.dst_schema
            if catalog in catalog_schemas:
                catalog_schemas[catalog].add(schema)
            else:
                catalog_schemas[catalog] = set({schema})
        return catalog_schemas

    def _is_valid_location(self, location: str) -> bool:
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

    def _create_catalog(self, catalog, catalog_storage) -> None:
        logger.info(f"Creating UC catalog: {catalog}")
        if catalog_storage == "metastore":
            self._ws.catalogs.create(catalog, comment="Created by UCX")
        else:
            self._ws.catalogs.create(catalog, storage_root=catalog_storage, comment="Created by UCX")

    def _create_schemas(self, catalog: str, schemas: set[str]) -> None:
        for schema in schemas:
            logger.info(f"Creating UC schema: {schema} in catalog: {catalog}")
            self._ws.schemas.create(schema, catalog, comment="Created by UCX")