import logging
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


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
        result = self._backend.fetch(f"SHOW TBLPROPERTIES {table.key}")
        for value in result:
            if value["key"] == TableMapping.UCX_SKIP_PROPERTY:
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
