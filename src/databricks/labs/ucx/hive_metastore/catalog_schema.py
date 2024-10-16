import collections
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import PurePath

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.grants import MigrateGrants
from databricks.labs.ucx.hive_metastore.mapping import TableMapping

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Catalog:
    """Represents a catalog from Unity Catalog.

    The Databricks SDK also comes with a representation for a catalog: `databricks.sdk.service.catalog.CatalogInfo`.
    However, we introduce this dataclass to have a minimal, extensible representation required for UCX.

    Docs:
        https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#securable-objects-in-unity-catalog
    """

    name: str
    """The catalog name"""

    @property
    def full_name(self) -> str:
        """The full name of the catalog.

        For a catalog, this is same as the attr:name as the catalog is the top of the object hierarchy (see doc link
        above).
        """
        return self.name

    @property
    def key(self) -> str:
        """Synonym for attr:full_name:."""
        return self.full_name

    @property
    def kind(self) -> str:
        """The object kind.

        Note:
            In the SDK this maps to attr:securable_type.

        TODO:
            https://github.com/databrickslabs/ucx/issues/2975
        """
        return "CATALOG"


@dataclass(frozen=True)
class Schema:
    """Represents a schema from Unity Catalog.

    The Databricks SDK also comes with a representation for a schema: `databricks.sdk.service.catalog.SchemaInfo`.
    However, we introduce this dataclass to have a minimal, extensible representation required for UCX.

    Docs:
        https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#securable-objects-in-unity-catalog
    """

    catalog: str
    """The catalog the schema is part of.

    Note:
        Maps to `SchemaInfo.catalog_name`, when introducing this class `catalog` is consistent with
        `databricks.labs.ucx.hive_metastore.tables.Table.catalog`.
    """

    name: str
    """The schema name"""

    @property
    def full_name(self) -> str:
        """The full name of the schema.

        For a schema, the second layer of the object hierarchy (see doc link above).
        """
        return f"{self.catalog}.{self.name}"

    @property
    def key(self) -> str:
        """Synonym for attr:full_name."""
        return self.full_name

    @property
    def kind(self) -> str:
        """The object kind.

        Below "DATABASE" is chosen as this is the kind used in the grants module. However, more consistent with
        Databricks documentation would be to use "SCHEMA" instead:
        https://docs.databricks.com/en/data-governance/table-acls/object-privileges.html#securable-objects-in-the-hive-metastore

        TODO:
            https://github.com/databrickslabs/ucx/issues/2975
        """
        return "DATABASE"  # TODO: https://github.com/databrickslabs/ucx/issues/2974


class CatalogSchema:

    def __init__(
        self,
        ws: WorkspaceClient,
        table_mapping: TableMapping,
        migrate_grants: MigrateGrants,
        ucx_catalog: str,
        *,
        timeout: dt.timedelta | None = dt.timedelta(seconds=30),
    ):
        self._ws = ws
        self._table_mapping = table_mapping
        self._migrate_grants = migrate_grants
        self._external_locations = list(self._ws.external_locations.list())
        self._ucx_catalog = ucx_catalog
        self._timeout = timeout

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

    def _get_catalog(self, catalog: Catalog) -> Catalog | None:
        """Get a catalog.

        Args:
            catalog (Catalog) : The catalog to get.

        Returns:
            Catalog : The catalog it got.
            None : If the catalog does not exist.
        """
        if self._timeout:
            get = retried(on=[NotFound], timeout=self._timeout)(self._ws.catalogs.get)
        else:
            get = self._ws.catalogs.get
        try:
            catalog_info = get(catalog.name)
            return Catalog(catalog_info.name)
        except (NotFound, TimeoutError):
            return None
        except DatabricksError as e:
            logger.warning(f"Unexpected error when getting catalog: {catalog.name}", exc_info=e)
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
        catalog_created = self._get_catalog(catalog)
        if catalog_created is None:
            raise NotFound(f"Created catalog '{catalog.name}' does not exist.")
        return catalog_created

    def _get_schema(self, schema: Schema) -> Schema | None:
        """Get a schema.

        Args:
            schema (Schema) : The schema to get.

        Returns:
            Schema : The schema it got.
            None : If the catalog does not exist.
        """
        if self._timeout:
            get = retried(on=[NotFound], timeout=self._timeout)(self._ws.schemas.get)
        else:
            get = self._ws.schemas.get
        try:
            schema_info = get(schema.full_name)
            return Schema(schema_info.catalog_name, schema_info.name)
        except (NotFound, TimeoutError):
            return None
        except DatabricksError as e:
            logger.warning(f"Unexpected error when getting schema: {schema.full_name}", exc_info=e)
            return None

    def _create_schema(self, schema: Schema) -> Schema:
        schema_existing = self._get_schema(schema)
        if schema_existing:
            logger.warning(f"Skipping already existing schema: {schema.full_name}")
            return schema_existing
        logger.info(f"Creating UC schema: {schema.full_name}")
        self._ws.schemas.create(schema.name, schema.catalog, comment="Created by UCX")
        schema_created = self._get_schema(schema)
        if schema_created is None:
            raise NotFound(f"Created schema '{schema.full_name}' does not exist.")
        return schema_created
