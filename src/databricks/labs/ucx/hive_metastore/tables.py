import logging
import re
import typing
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import PermissionsChange, SecurableType

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import ManyError, Threads
from databricks.labs.ucx.mixins.sql import Row

logger = logging.getLogger(__name__)


@dataclass
class Table:
    catalog: str
    database: str
    name: str
    object_type: str
    table_format: str

    location: str | None = None
    view_text: str | None = None
    upgraded_to: str | None = None

    storage_properties: str | None = None

    DBFS_ROOT_PREFIXES: typing.ClassVar[list[str]] = [
        "/dbfs/",
        "dbfs:/",
    ]

    DBFS_ROOT_PREFIX_EXCEPTIONS: typing.ClassVar[list[str]] = [
        "/dbfs/mnt",
        "dbfs:/mnt",
        "/dbfs/databricks-datasets",
        "dbfs:/databricks-datasets",
    ]

    @property
    def is_delta(self) -> bool:
        if self.table_format is None:
            return False
        return self.table_format.upper() == "DELTA"

    @property
    def key(self) -> str:
        return f"{self.catalog}.{self.database}.{self.name}".lower()

    @property
    def kind(self) -> str:
        return "VIEW" if self.view_text is not None else "TABLE"

    def _sql_external(self, catalog):
        return f"SYNC TABLE {catalog}.{self.database}.{self.name} FROM {self.key};"

    def _sql_managed(self, catalog):
        if not self.is_delta:
            msg = f"{self.key} is not DELTA: {self.table_format}"
            raise ValueError(msg)
        return f"CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name} DEEP CLONE {self.key};"

    def _sql_view(self, catalog):
        return f"CREATE VIEW IF NOT EXISTS {catalog}.{self.database}.{self.name} AS {self.view_text};"

    def uc_create_sql(self, catalog):
        if self.kind == "VIEW":
            return self._sql_view(catalog)
        elif self.object_type == "EXTERNAL":
            return self._sql_external(catalog)
        else:
            return self._sql_managed(catalog)

    def sql_alter_to(self, catalog):
        return (
            f"ALTER {self.kind} {self.key} SET"
            f" TBLPROPERTIES ('upgraded_to' = '{catalog}.{self.database}.{self.name}');"
        )

    def sql_alter_from(self, catalog):
        return (
            f"ALTER {self.kind} {catalog}.{self.database}.{self.name} SET"
            f" TBLPROPERTIES ('upgraded_from' = '{self.key}');"
        )

    def sql_unset_upgraded_to(self, catalog):
        return (
            f"ALTER {self.kind} `{catalog}`.`{self.database}`.`{self.name}` "
            f"UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        )

    def is_dbfs_root(self) -> bool:
        if not self.location:
            return False
        for exception in self.DBFS_ROOT_PREFIX_EXCEPTIONS:
            if self.location.startswith(exception):
                return False
        for prefix in self.DBFS_ROOT_PREFIXES:
            if self.location.startswith(prefix):
                return True
        return False


@dataclass
class TableError:
    catalog: str
    database: str
    name: str | None = None
    error: str | None = None


@dataclass
class MigrationCount:
    database: str
    managed_tables: int = 0
    external_tables: int = 0
    views: int = 0


class TablesCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema):
        """
        Initializes a TablesCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "tables", Table)

    def _all_databases(self) -> Iterator[Row]:
        yield from self._fetch("SHOW DATABASES")

    def snapshot(self) -> list[Table]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(partial(self._try_load), partial(self._crawl))

    @staticmethod
    def _parse_table_props(tbl_props: str) -> dict:
        pattern = r"([^,\[\]]+)=([^,\[\]]+)"
        key_value_pairs = re.findall(pattern, tbl_props)
        # Convert key-value pairs to dictionary
        return dict(key_value_pairs)

    def _try_load(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield Table(*row)

    def _crawl(self) -> Iterable[Table]:
        """Crawls and lists tables within the specified catalog and database.

        After performing initial scan of all tables, starts making parallel
        DESCRIBE TABLE EXTENDED queries for every table.

        Production tasks would most likely be executed through `tables.scala`
        within `crawl_tables` task due to `spark.sharedState.externalCatalog`
        lower-level APIs not requiring a roundtrip to storage, which is not
        possible for Azure storage with credentials supplied through Spark
        conf (see https://github.com/databrickslabs/ucx/issues/249).

        See also https://github.com/databrickslabs/ucx/issues/247
        """
        tasks = []
        catalog = "hive_metastore"
        for (database,) in self._all_databases():
            logger.debug(f"[{catalog}.{database}] listing tables")
            for _, table, _is_tmp in self._fetch(f"SHOW TABLES FROM {catalog}.{database}"):
                tasks.append(partial(self._describe, catalog, database, table))
        catalog_tables, errors = Threads.gather(f"listing tables in {catalog}", tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Detected {len(errors)} while scanning tables in {catalog}")
        return catalog_tables

    @staticmethod
    def _safe_norm(value: str | None, *, lower: bool = True) -> str | None:
        if not value:
            return None
        if lower:
            return value.lower()
        return value.upper()

    def _describe(self, catalog: str, database: str, table: str) -> Table | None:
        """Fetches metadata like table type, data format, external table location,
        and the text of a view if specified for a specific table within the given
        catalog and database.
        """
        full_name = f"{catalog}.{database}.{table}"
        try:
            logger.debug(f"[{full_name}] fetching table metadata")
            describe = {}
            for key, value, _ in self._fetch(f"DESCRIBE TABLE EXTENDED {full_name}"):
                describe[key] = value
            return Table(
                catalog=catalog.lower(),
                database=database.lower(),
                name=table.lower(),
                object_type=describe.get("Type", "UNKNOWN").upper(),
                table_format=describe.get("Provider", "UNKNOWN").upper(),
                location=describe.get("Location", None),
                view_text=describe.get("View Text", None),
                upgraded_to=self._parse_table_props(describe.get("Table Properties", "").lower()).get(
                    "upgraded_to", None
                ),
                storage_properties=self._parse_table_props(describe.get("Storage Properties", "").lower()),  # type: ignore[arg-type]
            )
        except Exception as e:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Couldn't fetch information for table {full_name} : {e}")
            return None


class TablesMigrate:
    def __init__(
        self,
        tc: TablesCrawler,
        ws: WorkspaceClient,
        backend: SqlBackend,
        default_catalog=None,
        database_to_catalog_mapping: dict[str, str] | None = None,
    ):
        self._tc = tc
        self._backend = backend
        self._ws = ws
        self._database_to_catalog_mapping = database_to_catalog_mapping
        self._default_catalog = self._init_default_catalog(default_catalog)
        self._seen_tables: dict[str, str] = {}

    @staticmethod
    def _init_default_catalog(default_catalog):
        if default_catalog:
            return default_catalog
        else:
            return "ucx_default"  # TODO : Fetch current workspace name and append it to the default catalog.

    def migrate_tables(self):
        self._init_seen_tables()
        tasks = []
        for table in self._tc.snapshot():
            target_catalog = self._default_catalog
            if self._database_to_catalog_mapping:
                target_catalog = self._database_to_catalog_mapping[table.database]
            tasks.append(partial(self._migrate_table, target_catalog, table))
        _, errors = Threads.gather("migrate tables", tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            # TODO: pick first X issues in the summary
            msg = f"Detected {len(errors)} errors: {'. '.join(str(e) for e in errors)}"
            raise ValueError(msg)

    def _migrate_table(self, target_catalog: str, table: Table):
        sql = table.uc_create_sql(target_catalog)
        logger.debug(f"Migrating table {table.key} to using SQL query: {sql}")
        target = f"{target_catalog}.{table.database}.{table.name}".lower()

        if self._table_already_upgraded(target):
            logger.info(f"Table {table.key} already upgraded to {self._seen_tables[target]}")
        elif table.object_type == "MANAGED":
            self._backend.execute(sql)
            self._backend.execute(table.sql_alter_to(target_catalog))
            self._backend.execute(table.sql_alter_from(target_catalog))
            self._seen_tables[target] = table.key
        elif table.object_type == "EXTERNAL":
            result = next(self._backend.fetch(sql))
            if result.status_code != "SUCCESS":
                raise ValueError(result.description)
            self._backend.execute(table.sql_alter_to(target_catalog))
            self._backend.execute(table.sql_alter_from(target_catalog))
            self._seen_tables[target] = table.key
        else:
            msg = f"Table {table.key} is a {table.object_type} and is not supported for migration yet"
            raise ValueError(msg)
        return True

    def _init_seen_tables(self):
        for catalog in self._ws.catalogs.list():
            for schema in self._ws.schemas.list(catalog_name=catalog.name):
                for table in self._ws.tables.list(catalog_name=catalog.name, schema_name=schema.name):
                    if table.properties is not None and "upgraded_from" in table.properties:
                        self._seen_tables[table.full_name.lower()] = table.properties["upgraded_from"].lower()

    def _table_already_upgraded(self, target) -> bool:
        return target in self._seen_tables

    def _get_tables_to_revert(self, schema: str | None = None, table: str | None = None) -> list[Table]:
        schema = schema.lower() if schema else None
        table = table.lower() if table else None
        upgraded_tables = []
        if table and not schema:
            logger.error("Cannot accept 'Table' parameter without 'Schema' parameter")
        if len(self._seen_tables) == 0:
            self._init_seen_tables()

        for cur_table in self._tc.snapshot():
            if schema and cur_table.database != schema:
                continue
            if table and cur_table.name != table:
                continue
            if cur_table.key in self._seen_tables.values():
                upgraded_tables.append(cur_table)
        return upgraded_tables

    def revert_migrated_tables(
        self, schema: str | None = None, table: str | None = None, *, delete_managed: bool = False
    ):
        upgraded_tables = self._get_tables_to_revert(schema=schema, table=table)
        # reverses the _seen_tables dictionary to key by the source table
        reverse_seen = {v: k for (k, v) in self._seen_tables.items()}
        tasks = []
        for upgraded_table in upgraded_tables:
            if upgraded_table.kind == "VIEW" or upgraded_table.object_type == "EXTERNAL" or delete_managed:
                tasks.append(partial(self._revert_migrated_table, upgraded_table, reverse_seen[upgraded_table.key]))
                continue
            logger.info(
                f"Skipping {upgraded_table.object_type} Table {upgraded_table.database}.{upgraded_table.name} "
                f"upgraded_to {upgraded_table.upgraded_to}"
            )
        Threads.strict("revert migrated tables", tasks)

    def _revert_migrated_table(self, table: Table, target_table_key: str):
        logger.info(
            f"Reverting {table.object_type} table {table.database}.{table.name} upgraded_to {table.upgraded_to}"
        )
        self._backend.execute(table.sql_unset_upgraded_to("hive_metastore"))
        self._backend.execute(f"DROP {table.kind} IF EXISTS {target_table_key}")

    def _get_revert_count(self, schema: str | None = None, table: str | None = None) -> list[MigrationCount]:
        upgraded_tables = self._get_tables_to_revert(schema=schema, table=table)

        table_by_database = defaultdict(list)
        for cur_table in upgraded_tables:
            table_by_database[cur_table.database].append(cur_table)

        migration_list = []
        for cur_database in table_by_database.keys():
            external_tables = 0
            managed_tables = 0
            views = 0
            for current_table in table_by_database[cur_database]:
                if current_table.upgraded_to is not None:
                    if current_table.kind == "VIEW":
                        views += 1
                        continue
                    if current_table.object_type == "EXTERNAL":
                        external_tables += 1
                        continue
                    if current_table.object_type == "MANAGED":
                        managed_tables += 1
                        continue
            migration_list.append(
                MigrationCount(
                    database=cur_database, managed_tables=managed_tables, external_tables=external_tables, views=views
                )
            )
        return migration_list

    def is_upgraded(self, schema: str, table: str) -> bool:
        result = self._backend.fetch(f"SHOW TBLPROPERTIES `{schema}`.`{table}`")
        for value in result:
            if value["key"] == "upgraded_to":
                logger.info(f"{schema}.{table} is set as upgraded")
                return True
        logger.info(f"{schema}.{table} is set as not upgraded")
        return False

    def print_revert_report(self, *, delete_managed: bool) -> bool | None:
        migrated_count = self._get_revert_count()
        if not migrated_count:
            logger.info("No migrated tables were found.")
            return False
        print("The following is the count of migrated tables and views found in scope:")
        print("Database                      | External Tables  | Managed Table    | Views            |")
        print("=" * 88)
        for count in migrated_count:
            print(f"{count.database:<30}| {count.external_tables:16} | {count.managed_tables:16} | {count.views:16} |")
        print("=" * 88)
        print("Migrated External Tables and Views (targets) will be deleted")
        if delete_managed:
            print("Migrated Manged Tables (targets) will be deleted")
        else:
            print("Migrated Manged Tables (targets) will be left intact.")
            print("To revert and delete Migrated Tables, add --delete_managed true flag to the command.")
        return True

    def migrate_uc_tables(
        self, from_catalog: str, from_schema: str, from_table: list[str], to_catalog: str, to_schema: str
    ):
        # a = self._validate_uc_objects("schema", f"{from_catalog}.{from_schema}")
        if self._validate_uc_objects("schema", f"{from_catalog}.{from_schema}") == 0:
            msg = f"schema {from_schema} not found in {from_catalog}"
            raise ManyError(msg)
        else:
            if self._validate_uc_objects("schema", f"{to_catalog}.{to_schema}") == 0:
                logger.warning(f"schema {to_schema} not found in {to_catalog}, creating...")
                self._backend.execute(f"create schema {to_catalog}.{to_schema}")
                logger.info(f"created schema {to_schema}.")
            tables = self._ws.tables.list(catalog_name=from_catalog, schema_name=from_schema)
            table_tasks = []
            view_tasks = []
            for table in tables:
                if table.name in from_table or from_table[0] == "*":
                    if self._validate_uc_objects("table", f"{to_catalog}.{to_schema}.{table.name}") == 1:
                        logger.warning(
                            f"table {from_table} already present in {from_catalog}.{from_schema}."
                            f" skipping this table..."
                        )
                        continue
                    if table.table_type and table.table_type.value in ("EXTERNAL", "MANAGED"):
                        table_tasks.append(
                            partial(
                                self._migrate_uc_table, from_catalog, from_schema, table.name, to_catalog, to_schema
                            )
                        )
                    else:
                        view_tasks.append(
                            partial(
                                self._migrate_uc_table,
                                from_catalog,
                                from_schema,
                                table.name,
                                to_catalog,
                                to_schema,
                                table.view_definition,
                            )
                        )
            _, errors = Threads.gather(name="creating tables", tasks=table_tasks)
            if len(errors) > 1:
                raise ManyError(errors)
            logger.info(f"migrated {len(list(_))} tables to the new schema {to_schema}.")
            _, errors = Threads.gather(name="creating views", tasks=view_tasks)
            if len(errors) > 1:
                raise ManyError(errors)
            logger.info(f"migrated {len(list(_))} views to the new schema {to_schema}.")

    def _migrate_uc_table(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
        view_text: str | None = None,
    ) -> bool:
        from_table_name = f"{from_catalog}.{from_schema}.{from_table}"
        to_table_name = f"{to_catalog}.{to_schema}.{from_table}"
        try:
            if not view_text:
                create_sql = str(next(self._backend.fetch(f"SHOW CREATE TABLE {from_table_name}"))[0])
                create_sql = create_sql.replace(from_catalog, to_catalog).replace(from_schema, to_schema)
                logger.debug(f"Creating table {from_table_name}.")
                self._backend.execute(create_sql)
            else:
                create_sql = f"CREATE VIEW {to_table_name} AS {view_text}"
                logger.debug(f"Creating view {to_table_name}.")
                self._backend.execute(create_sql)
            grants = self._ws.grants.get(securable_type=SecurableType.TABLE, full_name=from_table_name)
            if grants.privilege_assignments is None:
                return True
            grants_changes = [
                PermissionsChange(add=pair.privileges, principal=pair.principal)
                for pair in grants.privilege_assignments
            ]
            self._ws.grants.update(securable_type=SecurableType.TABLE, full_name=to_table_name, changes=grants_changes)
            return True
        except DatabricksError:
            logger.error(f"error applying permissions for {to_table_name}")
            return False

    def _validate_uc_objects(self, object_type: str, object_name: str) -> int:
        object_parts = object_name.split(".")
        if object_type == "table":
            query = (
                f"SELECT COUNT(*) as cnt FROM SYSTEM.INFORMATION_SCHEMA.TABLES WHERE "
                f"TABLE_CATALOG = '{object_parts[0]}' AND TABLE_SCHEMA = '{object_parts[1]}' AND "
                f"TABLE_NAME = '{object_parts[2]}'"
            )
        else:
            query = (
                f"SELECT COUNT(*) as cnt FROM SYSTEM.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME "
                f"= '{object_parts[0]}' "
                f"AND SCHEMA_NAME = '{object_parts[1]}'"
            )
        return next(self._backend.fetch(query))[0]
