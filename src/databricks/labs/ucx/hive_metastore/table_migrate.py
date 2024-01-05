import logging
from collections import defaultdict
from functools import partial

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import MigrationCount, Table

logger = logging.getLogger(__name__)


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
        self._tm = tm
        self._seen_tables: dict[str, str] = {}

    def migrate_tables(self):
        self._init_seen_tables()
        mapping_rules = self._get_mapping_rules()
        tasks = []
        for table in self._tc.snapshot():
            rule = mapping_rules.get(table.key)
            if not rule:
                logger.info(f"Skipping table {table.key} table doesn't exist in the mapping table.")
                continue
            tasks.append(partial(self._migrate_table, table, rule))
        Threads.strict("migrate tables", tasks)

    def _migrate_table(self, src_table: Table, rule: Rule):
        if self._table_already_upgraded(rule.as_uc_table_key):
            logger.info(f"Table {src_table.key} already upgraded to {rule.as_uc_table_key}")
            return True
        if src_table.object_type == "MANAGED":
            return self._migrate_managed_table(src_table, rule)
        if src_table.kind == "VIEW":
            return self._migrate_view(src_table, rule)
        if src_table.object_type == "EXTERNAL":
            return self._migrate_external_table(src_table, rule)
        return True

    def _migrate_external_table(self, src_table: Table, rule: Rule):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.uc_create_sql(target_table_key)
        logger.debug(f"Migrating external table {src_table.key} to using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        return True

    def _migrate_managed_table(self, src_table: Table, rule: Rule):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.uc_create_sql(target_table_key)
        logger.debug(f"Migrating managed table {src_table.key} to using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key))
        return True

    def _migrate_view(self, src_table: Table, rule: Rule):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.uc_create_sql(target_table_key)
        logger.debug(f"Migrating view {src_table.key} to using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key))
        return True

        msg = f"Table {src_table.key} is a {src_table.object_type} and is not supported for migration yet"
        logger.info(msg)

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
        self._init_seen_tables()
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
        self._backend.execute(table.sql_unset_upgraded_to())
        self._backend.execute(f"DROP {table.kind} IF EXISTS {target_table_key}")

    def _get_revert_count(self, schema: str | None = None, table: str | None = None) -> list[MigrationCount]:
        self._init_seen_tables()
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

    def _get_mapping_rules(self) -> dict[str, Rule]:
        mapping_rules: dict[str, Rule] = {}
        for rule in self._tm.load():
            mapping_rules[rule.as_hms_table_key] = rule
        return mapping_rules
