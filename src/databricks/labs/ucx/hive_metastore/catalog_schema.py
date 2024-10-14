import logging
from pathlib import PurePath

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore.grants import MigrateGrants
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.objects import Catalog, Schema

logger = logging.getLogger(__name__)


class CatalogSchema:

    def __init__(
        self, ws: WorkspaceClient, table_mapping: TableMapping, migrate_grants: MigrateGrants, ucx_catalog: str
    ):
        self._ws = ws
        self._table_mapping = table_mapping
        self._migrate_grants = migrate_grants
        self._external_locations = list(self._ws.external_locations.list())
        self._ucx_catalog = ucx_catalog

    def create_ucx_catalog(self, prompts: Prompts, *, properties: dict[str, str] | None = None) -> None:
        """Create the UCX catalog.

        Args:
            prompts : Prompts
                The prompts object to use for interactive input.
            properties : (dict[str, str] | None), default None
                The properties to pass to the catalog. If None, no properties are passed.
        """
        self._create_catalog_validate(Catalog(self._ucx_catalog), prompts, properties=properties)

    def create_all_catalogs_schemas(self, prompts: Prompts) -> None:
        catalogs_existing, schemas_existing = self._catalogs_schemas_from_unity_catalog()
        catalogs, schemas = self._catalogs_schemas_from_table_mapping()
        for src_schema, dst_catalog in catalogs:
            if dst_catalog in catalogs_existing:
                continue
            self._create_catalog_validate(dst_catalog, prompts, properties=None)
            # TODO: Apply ownership as last
            self._migrate_grants.apply(src_schema, dst_catalog)
        for src_schema, dst_schema in schemas:
            if dst_schema in schemas_existing:
                continue
            self._create_schema(dst_schema)
            self._migrate_grants.apply(src_schema, dst_schema)

    def _catalogs_schemas_from_unity_catalog(self) -> tuple[set[Catalog], set[Schema]]:
        """Generate a list of existing UC catalogs and schema."""
        logger.info("Listing existing UC catalogs and schemas")
        catalogs, schemas = set[Catalog](), set[Schema]()
        for catalog_info in self._ws.catalogs.list():
            if not catalog_info.name:
                continue
            catalog = Catalog(catalog_info.name)
            catalogs.add(catalog)
            for schema_info in self._ws.schemas.list(catalog.name, max_results=0):
                if not schema_info.name:
                    continue
                schema = Schema(schema_info.name, catalog_info.name)
                schemas.add(schema)
        return catalogs, schemas

    def _catalogs_schemas_from_table_mapping(self) -> tuple[list[tuple[Schema, Catalog]], list[tuple[Schema, Schema]]]:
        """Generate a list of catalogs and schema to be created from table mapping."""
        catalogs_seen, schemas_seen = set[Catalog](), set[Schema]()
        catalogs, schemas = [], []
        for mappings in self._table_mapping.load():
            src_schema = Schema(mappings.src_schema, "hive_metastore")
            dst_catalog = Catalog(mappings.catalog_name)
            if dst_catalog not in catalogs_seen:
                catalogs.append((src_schema, dst_catalog))
                catalogs_seen.add(dst_catalog)
            dst_schema = Schema(mappings.dst_schema, mappings.catalog_name)
            if dst_schema not in schemas_seen:
                schemas.append((src_schema, dst_schema))
                schemas_seen.add(dst_schema)
        return catalogs, schemas

    def _create_catalog_validate(
        self,
        catalog: Catalog,
        prompts: Prompts,
        *,
        properties: dict[str, str] | None,
    ) -> None:
        try:
            catalog_info = self._ws.catalogs.get(catalog.name)
        except NotFound:
            catalog_info = None
        if catalog_info:
            logger.warning(f"Skipping already existing catalog: {catalog_info.name}")
            return
        logger.info(f"Validating UC catalog: {catalog.name}")
        attempts = 3
        while True:
            catalog_storage = prompts.question(
                f"Please provide storage location url for catalog: {catalog.name}", default="metastore"
            )
            if self._validate_location(catalog_storage):
                break
            attempts -= 1
            if attempts == 0:
                raise NotFound(f"Failed to validate location for catalog: {catalog.name}")
        self._create_catalog(catalog, catalog_storage, properties=properties)

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

    def _create_catalog(self, catalog: Catalog, catalog_storage: str, *, properties: dict[str, str] | None) -> None:
        logger.info(f"Creating UC catalog: {catalog.name}")
        if catalog_storage == "metastore":
            self._ws.catalogs.create(catalog.name, comment="Created by UCX", properties=properties)
        else:
            self._ws.catalogs.create(
                catalog.name,
                storage_root=catalog_storage,
                comment="Created by UCX",
                properties=properties,
            )

    def _create_schema(self, schema: Schema) -> None:
        logger.info(f"Creating UC schema: {schema.full_name}")
        self._ws.schemas.create(schema.name, schema.catalog_name, comment="Created by UCX")
