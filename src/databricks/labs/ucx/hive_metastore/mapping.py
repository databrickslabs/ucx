import logging
import re
from collections.abc import Collection, Callable
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound, ResourceConflict, DatabricksError, InvalidParameterValue
from databricks.sdk.service.catalog import TableInfo, SchemaInfo

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.recon.base import TableIdentifier

logger = logging.getLogger(__name__)


@dataclass
class Rule:
    workspace_name: str
    catalog_name: str
    src_schema: str
    dst_schema: str
    src_table: str
    dst_table: str
    recon_tolerance_percent: int = 0  # threshold for row count comparison
    compare_rows: bool = False  # whether to compare row by row

    @classmethod
    def initial(cls, workspace_name: str, catalog_name: str, table: Table, recon_tolerance_percent: int) -> "Rule":
        return cls(
            workspace_name=workspace_name,
            catalog_name=catalog_name,
            src_schema=table.database,
            dst_schema=table.database,
            src_table=table.name,
            dst_table=table.name,
            recon_tolerance_percent=recon_tolerance_percent,
        )

    @classmethod
    def from_src_dst(cls, src_table: TableInfo, dst_schema: SchemaInfo) -> "Rule":
        return cls(
            workspace_name="workspace",
            catalog_name=str(dst_schema.catalog_name or ""),
            src_schema=str(src_table.schema_name or ""),
            dst_schema=str(dst_schema.name or ""),
            src_table=str(src_table.name or ""),
            dst_table=str(src_table.name or ""),
            recon_tolerance_percent=0,
        )

    def match(self, table: TableIdentifier) -> bool:
        return table.catalog == "hive_metastore" and self.src_schema == table.schema and self.src_table == table.table

    @property
    def as_uc_table_key(self) -> str:
        return f"{self.catalog_name}.{self.dst_schema}.{self.dst_table}"

    @property
    def as_hms_table_key(self) -> str:
        return f"hive_metastore.{self.src_schema}.{self.src_table}"


@dataclass
class TableToMigrate:
    src: Table
    rule: Rule

    def __hash__(self):
        return hash(self.src)

    def __eq__(self, other):
        return isinstance(other, TableToMigrate) and self.src == other.src


class TableMapping:
    FILENAME = 'mapping.csv'
    UCX_SKIP_PROPERTY = "databricks.labs.ucx.skip"

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        recon_tolerance_percent: int = 0,
    ):
        self._installation = installation
        self._ws = ws
        self._sql_backend = sql_backend
        self._recon_tolerance_percent = recon_tolerance_percent

    def current_tables(self, tables: TablesCrawler, workspace_name: str, catalog_name: str):
        tables_snapshot = list(tables.snapshot())
        if not tables_snapshot:
            msg = "No tables found. Please run: databricks labs ucx ensure-assessment-run"
            raise ValueError(msg)
        for table in tables_snapshot:
            yield Rule.initial(workspace_name, catalog_name, table, self._recon_tolerance_percent)

    def save(self, tables: TablesCrawler, workspace_info: WorkspaceInfo) -> str:
        workspace_name = workspace_info.current()
        default_catalog_name = re.sub(r"\W+", "_", workspace_name)
        current_tables = self.current_tables(tables, workspace_name, default_catalog_name)
        return self._installation.save(list(current_tables), filename=self.FILENAME)

    def load(self) -> list[Rule]:
        try:
            return self._installation.load(list[Rule], filename=self.FILENAME)
        except NotFound:
            msg = "Please run: databricks labs ucx create-table-mapping"
            raise ValueError(msg) from None

    def skip_table_or_view(self, schema_name: str, table_name: str, load_table: Callable[[str, str], Table | None]):
        # Marks a table to be skipped in the migration process by applying a table property
        try:
            table = load_table(schema_name, table_name)
            if table is None:
                raise NotFound("[TABLE_OR_VIEW_NOT_FOUND]")
            self._sql_backend.execute(
                f"ALTER {table.kind} {escape_sql_identifier(schema_name)}.{escape_sql_identifier(table_name)} SET TBLPROPERTIES('{self.UCX_SKIP_PROPERTY}' = true)"
            )
        except NotFound as err:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(err) or "[DELTA_TABLE_NOT_FOUND]" in str(err):
                logger.error(f"Failed to apply skip marker for Table {schema_name}.{table_name}. Table not found.")
            else:
                logger.error(
                    f"Failed to apply skip marker for Table {schema_name}.{table_name}: {err!s}", exc_info=True
                )
        except BadRequest as err:
            logger.error(f"Failed to apply skip marker for Table {schema_name}.{table_name}: {err!s}", exc_info=True)

    def unskip_table_or_view(
        self, schema_name: str, table_name: str, load_table: Callable[[str, str], Table | None]
    ) -> None:
        """Removes skip mark from the table property.

        Args:
            schema_name (str): The schema name of the table to be unskipped.
            table_name (str): The table name of the table to be unskipped.
            load_table (Callable[[str, str], Table | None]): A function that loads a table from the metastore.
        """
        table = load_table(schema_name, table_name)
        if table is None:
            logger.error(
                f"Failed to remove skip marker from table: {schema_name}.{table_name}. Table not found.",
            )
            return
        try:
            self._sql_backend.execute(
                f"ALTER {table.kind} {escape_sql_identifier(table.full_name)} UNSET TBLPROPERTIES IF EXISTS('{self.UCX_SKIP_PROPERTY}');"
            )
        except (NotFound, BadRequest) as e:
            logger.error(f"Failed to remove skip marker from table: {table.full_name}", exc_info=e)

    def skip_schema(self, schema: str):
        # Marks a schema to be skipped in the migration process by applying a table property
        try:
            self._sql_backend.execute(
                f"ALTER SCHEMA {escape_sql_identifier(schema)} SET DBPROPERTIES('{self.UCX_SKIP_PROPERTY}' = true)"
            )
        except NotFound as err:
            if "[SCHEMA_NOT_FOUND]" in str(err):
                logger.error(f"Failed to apply skip marker for Schema {schema}. Schema not found.")
            else:
                logger.error(err)
        except BadRequest as err:
            logger.error(err)

    def unskip_schema(self, schema: str) -> None:
        """Removes skip mark from the schema property.

        Args:
            schema (str): The schema name of the table to be unskipped.
        """
        try:
            self._sql_backend.execute(
                f"ALTER SCHEMA hive_metastore.{escape_sql_identifier(schema)} UNSET DBPROPERTIES IF EXISTS('{self.UCX_SKIP_PROPERTY}');"
            )
        except (NotFound, BadRequest) as e:
            logger.error(f"Failed to remove skip marker from schema: {schema}.", exc_info=e)

    def get_tables_to_migrate(self, tables_crawler: TablesCrawler) -> Collection[TableToMigrate]:
        rules = self.load()
        # Getting all the source tables from the rules
        databases_in_scope = self._get_databases_in_scope({rule.src_schema for rule in rules})
        crawled_tables_keys = {crawled_table.key: crawled_table for crawled_table in tables_crawler.snapshot()}
        tasks = []
        for rule in rules:
            if rule.as_hms_table_key not in crawled_tables_keys:
                logger.info(f"Table {rule.as_hms_table_key} in the mapping doesn't show up in assessment")
                continue
            if rule.src_schema not in databases_in_scope:
                logger.info(f"Table {rule.as_hms_table_key} is in a database that was marked to be skipped")
                continue
            if crawled_tables_keys[rule.as_hms_table_key].is_databricks_dataset:
                logger.info(f"Table {rule.as_hms_table_key} is a db demo dataset and will not be migrated")
                continue
            tasks.append(
                partial(self._get_table_in_scope_task, TableToMigrate(crawled_tables_keys[rule.as_hms_table_key], rule))
            )

        return Threads.strict("checking all database properties", tasks)

    def _get_databases_in_scope(self, databases: set[str]):
        tasks = []
        for database in databases:
            tasks.append(partial(self._get_database_in_scope_task, database))
        return Threads.strict("checking databases for skip property", tasks)

    def _get_database_in_scope_task(self, database: str) -> str | None:
        if database.startswith("mounted_"):
            return database
        describe = {}
        try:
            for value in self._sql_backend.fetch(f"DESCRIBE SCHEMA EXTENDED {escape_sql_identifier(database)}"):
                describe[value["database_description_item"]] = value["database_description_value"]
        except NotFound:
            logger.warning(
                f"Schema hive_metastore.{database} no longer exists. Skipping its properties check and migration."
            )
            return None
        properties = describe.get("Properties", "")
        if not properties:
            return database
        if self.UCX_SKIP_PROPERTY in TablesCrawler.parse_database_props(properties.lower()):
            logger.info(f"Database {database} is marked to be skipped")
            return None
        return database

    def _get_table_in_scope_task(self, table_to_migrate: TableToMigrate) -> TableToMigrate | None:
        table = table_to_migrate.src
        rule = table_to_migrate.rule

        if self.exists_in_uc(table, rule.as_uc_table_key):
            logger.info(f"The intended target for {table.key}, {rule.as_uc_table_key}, already exists.")
            return None
        properties = self._get_table_properties(table)
        if not properties:
            return None

        for value in properties:
            if value["key"] == self.UCX_SKIP_PROPERTY:
                logger.info(f"{table.key} is marked to be skipped")
                return None
            if value["key"] == "upgraded_to":
                logger.info(f"{table.key} is set as upgraded to {value['value']}")
                if self.exists_in_uc(table, value["value"]):
                    logger.info(
                        f"The table {table.key} was previously migrated to {value['value']}. "
                        f"To revert the table and allow it to be migrated again use the CLI command:"
                        f"databricks labs ucx revert --schema {table.database} --table {table.name}"
                    )
                    return None
                logger.info(f"The upgrade_to target for {table.key} is missing. Unsetting the upgrade_to property")
                try:
                    self._sql_backend.execute(table.sql_unset_upgraded_to())
                except DatabricksError as err:
                    logger.warning(f"Failed to unset upgraded_to property for Table {table.key}: {err}")

        return table_to_migrate

    def _get_table_properties(self, table: Table):
        table_identifier = f"{escape_sql_identifier(table.database)}.{escape_sql_identifier(table.name)}"
        if table.is_table_in_mount:
            table_identifier = f"delta.`{table.location}`"

        try:
            return self._sql_backend.fetch(f"SHOW TBLPROPERTIES {table_identifier}")
        except DatabricksError as err:
            logger.warning(f"Failed to get properties for Table {table.key}: {err}")
            return None

    def exists_in_uc(self, src_table: Table, target_key: str):
        # Attempts to get the target table info from UC returns True if it exists.
        table_info = self._try_get_table_in_uc(target_key)
        if not table_info:
            return False
        # Corner case for tables in mounts where the table exists in UC, but the location is not the same
        # from the one provided in the mapping
        if src_table.is_table_in_mount and table_info.storage_location != src_table.location:
            raise ResourceConflict(
                f"Expected to be migrated from {src_table.key}, but got {table_info.storage_location}. "
                "You can skip this error using the CLI command: "
                "databricks labs ucx skip "
                f"--schema {src_table.database} --table {src_table.name}"
            )
        if not table_info.properties:
            return True
        upgraded_from = table_info.properties.get("upgraded_from")
        if upgraded_from and upgraded_from != src_table.key and not src_table.is_table_in_mount:
            raise ResourceConflict(
                f"Expected to be migrated from {src_table.key}, but got {upgraded_from}. "
                "You can skip this error using the CLI command: "
                "databricks labs ucx skip "
                f"--schema {src_table.database} --table {src_table.name}"
            )
        return True

    def _try_get_table_in_uc(self, target_key: str):
        try:
            return self._ws.tables.get(target_key)
        except (NotFound, InvalidParameterValue):
            return None
