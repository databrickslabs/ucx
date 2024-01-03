import logging
import re
import typing
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import Threads
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
    ]

    DATABRICKS_DATASETS_PREFIXES: typing.ClassVar[list[str]] = [
        "/dbfs/databricks-datasets",
        "dbfs:/databricks-datasets",
    ]

    @property
    def is_delta(self) -> bool:
        if self.table_format is None:
            return False
        return self.table_format.upper() == "DELTA"

    @property
    def is_supported_for_sync(self) -> bool:
        if self.table_format is None:
            return False
        return self.table_format.upper() in ("DELTA", "PARQUET", "CSV", "JSON", "ORC", "TEXT")

    @property
    def key(self) -> str:
        return f"{self.catalog}.{self.database}.{self.name}".lower()

    @property
    def kind(self) -> str:
        return "VIEW" if self.view_text is not None else "TABLE"

    def sql_external(self, target_key):
        return f"SYNC TABLE {target_key} FROM {self.key};"

    def sql_managed(self, target_key):
        if not self.is_delta:
            msg = f"{self.key} is not DELTA: {self.table_format}"
            raise ValueError(msg)
        return f"CREATE TABLE IF NOT EXISTS {target_key} DEEP CLONE {self.key};"

    def sql_view(self, target_key):
        return f"CREATE VIEW IF NOT EXISTS {target_key} AS {self.view_text};"

    def sql_unset_upgraded_to(self, catalog):
        return (
            f"ALTER {self.kind} `{catalog}`.`{self.database}`.`{self.name}` "
            f"UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        )

    @property
    def is_dbfs_root(self) -> bool:
        if not self.location:
            return False
        for exception in self.DBFS_ROOT_PREFIX_EXCEPTIONS:
            if self.location.startswith(exception):
                return False
        for databricks_dataset in self.DATABRICKS_DATASETS_PREFIXES:
            if self.location.startswith(databricks_dataset):
                return False
        for prefix in self.DBFS_ROOT_PREFIXES:
            if self.location.startswith(prefix):
                return True
        return False

    @property
    def is_databricks_dataset(self) -> bool:
        if not self.location:
            return False
        for databricks_dataset in self.DATABRICKS_DATASETS_PREFIXES:
            if self.location.startswith(databricks_dataset):
                return True
        return False


@dataclass
class TableError:
    catalog: str
    database: str
    name: str | None = None
    error: str | None = None


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


@dataclass
class MigrationCount:
    database: str
    managed_tables: int = 0
    external_tables: int = 0
    views: int = 0


class TableMigrationStrategy:
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        source_table: Table,
        target_table_key: str,
    ):
        self._backend = backend
        self._ws = ws
        self._source_table = source_table
        self._target_table_key = target_table_key

    @abstractmethod
    def migrate_object(self):
        raise NotImplementedError


class ExternalTableTargetStrategy(TableMigrationStrategy):
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        source_table: Table,
        target_table_key: str,
    ):
        super().__init__(ws, backend, source_table, target_table_key)

    def migrate_object(self):
        source_table = self._source_table
        target_table_key = self._target_table_key
        sql = source_table.sql_external(target_table_key)
        logger.debug(f"Migrating table {source_table.key} to {target_table_key} (EXTERNAL) using SQL query: {sql}")
        self._backend.execute(sql)


class DBFSRootToManagedStrategy(TableMigrationStrategy):
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        source_table: Table,
        target_table_key: str,
    ):
        super().__init__(ws, backend, source_table, target_table_key)

    def migrate_object(self):
        source_table = self._source_table
        target_table_key = self._target_table_key
        sql = source_table.sql_managed(target_table_key)
        logger.debug(
            f"Migrating table {source_table.key} to {target_table_key} (DBFS Root to Managed) using SQL query: {sql}"
        )
        self._backend.execute(sql)
        self._backend.execute(TablesMigrate.sql_alter_to(source_table, target_table_key))
        self._backend.execute(TablesMigrate.sql_alter_from(source_table, target_table_key))
        return True


class MigrateViewStrategy(TableMigrationStrategy):
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        source_table: Table,
        target_table_key: str,
    ):
        super().__init__(ws, backend, source_table, target_table_key)

    def migrate_object(self):
        source_view = self._source_table
        target_view_key = self._target_table_key
        sql = source_view.sql_view(target_view_key)
        logger.debug(f"Migrating table {source_view.key} to {target_view_key} (VIEW) using SQL query: {sql}")
        self._backend.execute(sql)
        self._backend.execute(TablesMigrate.sql_alter_to(source_view, target_view_key))
        self._backend.execute(TablesMigrate.sql_alter_from(source_view, target_view_key))
        return True


class TablesMigrate:
    from databricks.labs.ucx.hive_metastore.mapping import TableMapping

    def __init__(
        self,
        tc: TablesCrawler,
        ws: WorkspaceClient,
        backend: SqlBackend,
        tm: TableMapping,
    ):
        self._tc = tc
        self._backend = backend
        self._ws = ws
        self._seen_tables: dict[str, str] = {}
        mapping_rules: dict[str, str] = {}
        for rule in tm.load():
            mapping_rules[
                f"hive_metastore.{rule.src_schema}.{rule.src_table}"
            ] = f"{rule.catalog_name}.{rule.dst_schema}.{rule.dst_table}"
        self._mapping_rules = mapping_rules

    @staticmethod
    def _init_default_catalog(default_catalog):
        if default_catalog:
            return default_catalog
        else:
            return "ucx_default"  # TODO : Fetch current workspace name and append it to the default catalog.

    @staticmethod
    def sql_alter_to(from_table: Table, to_table_key: str):
        return f"ALTER {from_table.kind} {from_table.key} SET TBLPROPERTIES ('upgraded_to' = '{to_table_key}');"

    @staticmethod
    def sql_alter_from(from_table: Table, to_table_key: str):
        return f"ALTER {from_table.kind} {to_table_key} SET TBLPROPERTIES ('upgraded_from' = '{from_table.key}');"

    def migrate_tables(self):
        self._init_seen_tables()
        tasks = []
        for table in self._tc.snapshot():
            target_table = self._mapping_rules.get(table.key)
            if not target_table:
                logger.info(f"Table {table.key} doesn't not have a valid mapping and will be skipped")
                continue
            if self._table_already_upgraded(target_table):
                logger.info(f"Table {table.key} has a target {target_table} that is marked as an upgraded table.")
                continue
            tasks.append(partial(self._migrate_table, table, target_table))
        Threads.strict("migrate tables", tasks)

    def _get_migration_strategy(self, source_table, target_table_key) -> TableMigrationStrategy:
        if source_table.kind == "VIEW":
            return MigrateViewStrategy(self._ws, self._backend, source_table, target_table_key)
        if source_table.is_databricks_dataset:
            msg = (
                f"Table {source_table.key} is a reference to a databricks "
                f"dataset {source_table.location} and will not be migrated"
            )
            raise ValueError(msg)
        if source_table.is_dbfs_root and source_table.is_delta:
            return DBFSRootToManagedStrategy(self._ws, self._backend, source_table, target_table_key)
        if source_table.is_supported_for_sync:
            return ExternalTableTargetStrategy(self._ws, self._backend, source_table, target_table_key)
        else:
            msg = (
                f"Table {source_table.key} of type {source_table.object_type} and format "
                f"{source_table.table_format} is not currently supported for migration."
            )
            raise ValueError(msg)

    def _migrate_table(self, source_table: Table, target_table: str):
        if self._is_marked_for_skip(source_table):
            msg = f"Table {source_table.key} is marked to be skipped and will not be upgraded"
            raise ValueError(msg)
        migration_strategy = self._get_migration_strategy(source_table, target_table)
        migration_strategy.migrate_object()
        self._seen_tables[target_table] = source_table.key

    def _init_seen_tables(self):
        for catalog in self._ws.catalogs.list():
            for schema in self._ws.schemas.list(catalog_name=catalog.name):
                for table in self._ws.tables.list(catalog_name=catalog.name, schema_name=schema.name):
                    if table.properties is not None and "upgraded_from" in table.properties:
                        self._seen_tables[table.full_name.lower()] = table.properties["upgraded_from"].lower()

    def _table_already_upgraded(self, target: str) -> bool:
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

    def _get_upgrade_count(self, schema: str | None = None, table: str | None = None) -> list[MigrationCount]:
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

    def _is_upgraded(self, schema: str, table: str) -> bool:
        result = self._backend.fetch(f"SHOW TBLPROPERTIES `{schema}`.`{table}`")
        for value in result:
            if value["key"] == "upgraded_to":
                logger.info(f"{schema}.{table} is set as upgraded")
                return True
        logger.info(f"{schema}.{table} is set as not upgraded")
        return False

    def _is_marked_for_skip(self, table: Table) -> bool:
        import databricks.labs.ucx.hive_metastore

        result = self._backend.fetch(f"SHOW TBLPROPERTIES {table.key}")
        for value in result:
            if value["key"] == databricks.labs.ucx.hive_metastore.TableMapping.UCX_SKIP_PROPERTY:
                logger.info(f"{table.key} is set to be skipped")
                return True
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
