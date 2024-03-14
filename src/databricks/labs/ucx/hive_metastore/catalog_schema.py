import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.mapping import TableMapping

logger = logging.getLogger(__name__)


class CatalogSchema:
    def __init__(self, ws: WorkspaceClient, table_mapping: TableMapping, prompts: Prompts):
        self._ws = ws
        self._table_mapping = table_mapping
        self._prompts = prompts

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, installation: Installation, prompts: Prompts):
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        table_mapping = TableMapping(installation, ws, sql_backend)
        return cls(ws, table_mapping, prompts)

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

    def _prepare(self) -> tuple[set[str], dict[str, set[str]]]:
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

    def _create(self, catalogs, schemas):
        logger.info("Creating UC catalogs and schemas.")
        # create catalogs
        for catalog_name in catalogs:
            catalog_storage = self._prompts.question(
                f"Please provide storage location url for catalog:{catalog_name}.", default="metastore"
            )
            if catalog_storage == "metastore":
                self._ws.catalogs.create(catalog_name, comment="Created by UCX")
                continue
            self._ws.catalogs.create(catalog_name, storage_root=catalog_storage, comment="Created by UCX")

        # create schemas
        for catalog_name, schema_names in schemas.items():
            for schema_name in schema_names:
                self._ws.schemas.create(schema_name, catalog_name, comment="Created by UCX")

    def create_catalog_schema(self):
        candidate_catalogs, candidate_schemas = self._prepare()
        self._create(candidate_catalogs, candidate_schemas)
