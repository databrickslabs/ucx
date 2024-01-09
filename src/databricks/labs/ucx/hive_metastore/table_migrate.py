import logging
from collections import defaultdict
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.catalog import PermissionsChange, SecurableType, TableType

from databricks.labs.ucx.framework.crawlers import SqlBackend
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
        tables_to_migrate = self._tm.get_tables_to_migrate(self._tc)
        tasks = []
        for table in tables_to_migrate:
            tasks.append(partial(self._migrate_table, table.src, table.rule))
        Threads.strict("migrate tables", tasks)

    def _migrate_table(self, src_table: Table, rule: Rule):
        if self._table_already_upgraded(rule.as_uc_table_key):
            logger.info(f"Table {src_table.key} already upgraded to {rule.as_uc_table_key}")
            return True
        if src_table.kind == "TABLE" and src_table.table_format == "DELTA" and src_table.is_dbfs_root:
            return self._migrate_dbfs_root_table(src_table, rule)
        if src_table.kind == "TABLE" and src_table.is_format_supported_for_sync:
            return self._migrate_external_table(src_table, rule)
        if src_table.kind == "VIEW":
            return self._migrate_view(src_table, rule)
        logger.info(f"Table {src_table.key} is not supported for migration")
        return True

    def _migrate_external_table(self, src_table: Table, rule: Rule):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_external(target_table_key)
        logger.debug(f"Migrating external table {src_table.key} to using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        return True

    def _migrate_dbfs_root_table(self, src_table: Table, rule: Rule):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_dbfs(target_table_key)
        logger.debug(f"Migrating managed table {src_table.key} to using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key))
        return True

    def _migrate_view(self, src_table: Table, rule: Rule):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_view(target_table_key)
        logger.debug(f"Migrating view {src_table.key} to using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key))
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


class TableMove:
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
    ):
        self._backend = backend
        self._ws = ws

    def move_tables(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
        del_table: bool,  # noqa: FBT001
    ):
        try:
            self._ws.schemas.get(f"{from_catalog}.{from_schema}")
        except NotFound:
            logger.error(f"schema {from_schema} not found in catalog {from_catalog}, enter correct schema details.")
            return
        try:
            self._ws.schemas.get(f"{to_catalog}.{to_schema}")
        except NotFound:
            logger.warning(f"schema {to_schema} not found in {to_catalog}, creating...")
            self._ws.schemas.create(to_schema, to_catalog)

        tables = self._ws.tables.list(from_catalog, from_schema)
        table_tasks = []
        view_tasks = []
        filtered_tables = [table for table in tables if from_table in [table.name, "*"]]
        for table in filtered_tables:
            try:
                self._ws.tables.get(f"{to_catalog}.{to_schema}.{table.name}")
                logger.warning(
                    f"table {from_table} already present in {from_catalog}.{from_schema}. skipping this table..."
                )
                continue
            except NotFound:
                if table.table_type and table.table_type in (TableType.EXTERNAL, TableType.MANAGED):
                    table_tasks.append(
                        partial(
                            self._move_table, from_catalog, from_schema, table.name, to_catalog, to_schema, del_table
                        )
                    )
                else:
                    view_tasks.append(
                        partial(
                            self._move_view,
                            from_catalog,
                            from_schema,
                            table.name,
                            to_catalog,
                            to_schema,
                            del_table,
                            table.view_definition,
                        )
                    )
        Threads.strict("creating tables", table_tasks)
        logger.info(f"moved {len(list(table_tasks))} tables to the new schema {to_schema}.")
        Threads.strict("creating views", view_tasks)
        logger.info(f"moved {len(list(view_tasks))} views to the new schema {to_schema}.")

    def _move_table(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
        del_table: bool,  # noqa: FBT001
    ) -> bool:
        from_table_name = f"{from_catalog}.{from_schema}.{from_table}"
        to_table_name = f"{to_catalog}.{to_schema}.{from_table}"
        try:
            create_sql = str(next(self._backend.fetch(f"SHOW CREATE TABLE {from_table_name}"))[0])
            create_table_sql = create_sql.replace(f"CREATE TABLE {from_table_name}", f"CREATE TABLE {to_table_name}")
            logger.debug(f"Creating table {to_table_name}.")
            self._backend.execute(create_table_sql)
            grants = self._ws.grants.get(SecurableType.TABLE, from_table_name)
            if grants.privilege_assignments is None:
                return True
            grants_changes = [
                PermissionsChange(pair.privileges, pair.principal) for pair in grants.privilege_assignments
            ]
            self._ws.grants.update(SecurableType.TABLE, to_table_name, changes=grants_changes)
            if del_table:
                logger.info(f"dropping source table {from_table_name}")
                drop_sql = f"DROP TABLE {from_table_name}"
                self._backend.execute(drop_sql)
            return True

        except NotFound as err:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(err):
                logger.error(f"Could not find table {from_table_name}. Table not found.")
            else:
                logger.error(err)
        except PermissionDenied:
            logger.error(f"error applying permissions for table {to_table_name}")
        return False

    def _move_view(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
        del_view: bool,  # noqa: FBT001
        view_text: str | None = None,
    ) -> bool:
        from_table_name = f"{from_catalog}.{from_schema}.{from_table}"
        to_table_name = f"{to_catalog}.{to_schema}.{from_table}"
        try:
            create_sql = f"CREATE VIEW {to_table_name} AS {view_text}"
            logger.debug(f"Creating view {to_table_name}.")
            self._backend.execute(create_sql)
            grants = self._ws.grants.get(SecurableType.TABLE, from_table_name)
            if grants.privilege_assignments is None:
                return True
            grants_changes = [
                PermissionsChange(pair.privileges, pair.principal) for pair in grants.privilege_assignments
            ]
            self._ws.grants.update(SecurableType.TABLE, to_table_name, changes=grants_changes)
            if del_view:
                logger.info(f"dropping source view {from_table_name}")
                drop_sql = f"DROP VIEW {from_table_name}"
                self._backend.execute(drop_sql)
            return True
        except NotFound as err:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(err):
                logger.error(f"Could not find view {from_table_name}. View not found.")
            else:
                logger.error(err)
        except PermissionDenied:
            logger.error(f"error applying permissions for view {to_table_name}")
        return False
