import collections
import datetime as dt
import logging
from pathlib import PurePath

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.grants import MigrateGrants
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.objects import Catalog, Schema

logger = logging.getLogger(__name__)


class CatalogSchema:

    def __init__(
        self,
        ws: WorkspaceClient,
        table_mapping: TableMapping,
        migrate_grants: MigrateGrants,
        ucx_catalog: str,
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

    def create_all_catalogs_schemas(self, prompts: Prompts, *, properties: dict[str, str] | None = None) -> None:
        """Create all UC catalogs and schemas reference by the table mapping file.

        After creation, the grants from the HIVE metastore schemas are applied to the matching UC catalogs and schemas.
        """
        catalogs, schemas = self._catalogs_schemas_from_table_mapping()
        for dst_catalog, src_schemas in catalogs.items():
            self._create_catalog_validate(dst_catalog, prompts, properties=properties)
        for dst_schema, src_schemas in schemas.items():
            self._create_schema(dst_schema)
            for src_schema in src_schemas:
                self._migrate_grants.apply(src_schema, dst_schema)
        # Apply catalog grants as last to avoid transferring ownership before schema grants are applied
        for dst_catalog, src_schemas in catalogs.items():
            for src_schema in src_schemas:
                self._migrate_grants.apply(src_schema, dst_catalog)

    def _catalogs_schemas_from_table_mapping(self) -> tuple[dict[Catalog, set[Schema]], dict[Schema, set[Schema]]]:
        """Generate a list of catalogs and schema to be created from table mapping.

        For applying grants after creating the catalogs and schemas, we track the HIVE metastore schemas from which the
        UC catalog or schema is mapped.

        :returns
            dict[Catalog, set[Schema]] : The UC catalogs to create with the schemas it is mapped from.
            dict[Schema, set[Schema]] : The UC schemas to create with the schemas it is mapped from.
        """
        catalogs, schemas = collections.defaultdict(set), collections.defaultdict(set)
        for mappings in self._table_mapping.load():
            src_schema = Schema("hive_metastore", mappings.src_schema)
            dst_catalog = Catalog(mappings.catalog_name)
            dst_schema = Schema(mappings.catalog_name, mappings.dst_schema)
            catalogs[dst_catalog].add(src_schema)
            schemas[dst_schema].add(src_schema)
        return catalogs, schemas

    def _create_catalog_validate(
        self,
        catalog: Catalog,
        prompts: Prompts,
        *,
        properties: dict[str, str] | None,
    ) -> Catalog:
        catalog_existing = self._get_catalog(catalog)
        if catalog_existing:
            logger.warning(f"Skipping already existing catalog: {catalog.name}")
            return catalog_existing
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
        return self._create_catalog(catalog, catalog_storage, properties=properties)

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

    def _get_catalog(
        self,
        catalog: Catalog,
        *,
        timeout: dt.timedelta | None = None,
    ) -> Catalog | None:
        """Get a catalog.

        Args:
            catalog (Catalog) : The catalog to get.
            timeout (dt.timedelta) : Timeout to wait before concluding the catalog does not exist. If None, no timeout
                is applied. Defaults to `None`.

        Returns:
            Catalog : The catalog it got.
            None : If the catalog does not exist.
        """
        if timeout:
            get = retried(on=[NotFound], timeout=timeout)(self._ws.catalogs.get)
        else:
            get = self._ws.catalogs.get
        try:
            catalog_info = get(catalog.name)
            return Catalog(catalog_info.name)
        except (NotFound, TimeoutError):
            return None

    def _create_catalog(
        self,
        catalog: Catalog,
        catalog_storage: str,
        *,
        properties: dict[str, str] | None,
    ) -> Catalog:
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
        catalog_created = self._get_catalog(catalog, timeout=dt.timedelta(seconds=10))
        if catalog_created is None:
            raise NotFound(f"Created catalog '{catalog.name}' does not exist.")
        return catalog_created

    def _create_schema(self, schema: Schema) -> None:
        try:
            schema_info = self._ws.schemas.get(schema.full_name)
        except NotFound:
            schema_info = None
        if schema_info:
            logger.warning(f"Skipping already existing schema: {schema_info.full_name}")
            return
        logger.info(f"Creating UC schema: {schema.full_name}")
        self._ws.schemas.create(schema.name, schema.catalog, comment="Created by UCX")
