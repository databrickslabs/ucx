from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    PermissionsChange,
    Privilege,
    SecurableType,
    TableType,
)

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migrate import logger


class TableMove:
    def __init__(self, ws: WorkspaceClient, backend: SqlBackend):
        self._backend = backend
        self._fetch = backend.fetch
        self._execute = backend.execute
        self._ws = ws

    def move(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
        *,
        del_table: bool,
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
                    f"table {from_table} already present in {to_catalog}.{to_schema}. skipping this table..."
                )
                continue
            except NotFound:
                if table.table_type and table.table_type in (TableType.EXTERNAL, TableType.MANAGED):
                    table_tasks.append(
                        partial(
                            self._move_table,
                            from_catalog,
                            from_schema,
                            table.name,
                            to_catalog,
                            to_schema,
                            table.table_type,
                            del_table=del_table,
                        )
                    )
                    continue
                if table.view_definition:
                    view_tasks.append(
                        partial(
                            self._move_view,
                            from_catalog,
                            from_schema,
                            table.name,
                            to_catalog,
                            to_schema,
                            del_view=del_table,
                            view_text=table.view_definition,
                        )
                    )
                    continue
                logger.warning(
                    f"table {from_table} was not identified as a valid table or view. skipping this table..."
                )
        Threads.strict("Creating tables", table_tasks)
        logger.info(f"Moved {len(list(table_tasks))} tables to the new schema {to_schema}.")
        Threads.strict("Creating views", view_tasks)
        logger.info(f"Moved {len(list(view_tasks))} views to the new schema {to_schema}.")

    def alias_tables(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
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
        alias_tasks = []
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
                    alias_tasks.append(
                        partial(self._alias_table, from_catalog, from_schema, table.name, to_catalog, to_schema)
                    )
                    continue
                if table.view_definition:
                    alias_tasks.append(
                        partial(
                            self._move_view,
                            from_catalog,
                            from_schema,
                            table.name,
                            to_catalog,
                            to_schema,
                            del_view=False,
                            view_text=table.view_definition,
                        )
                    )
                    continue
                logger.warning(
                    f"table {from_table} was not identified as a valid table or view. skipping this table..."
                )
        Threads.strict("Creating aliases", alias_tasks)
        logger.info(f"Created {len(list(alias_tasks))} table and view aliases in the new schema {to_schema}.")

    def _move_table(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
        table_type: str,
        *,
        del_table: bool,
    ) -> bool:
        from_table_name = f"{from_catalog}.{from_schema}.{from_table}"
        to_table_name = f"{to_catalog}.{to_schema}.{from_table}"
        try:
            self._recreate_table(
                from_table_name, to_table_name, is_managed=table_type == TableType.MANAGED, del_table=del_table
            )
            self._reapply_grants(from_table_name, to_table_name)
        except NotFound as err:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(err) or "[DELTA_TABLE_NOT_FOUND]" in str(err):
                logger.error(f"Could not find table {from_table_name}. Table not found.")
            else:
                logger.error(f"Failed to move table {from_table_name}: {err!s}", exc_info=True)
        return False

    def _alias_table(
        self,
        from_catalog: str,
        from_schema: str,
        from_table: str,
        to_catalog: str,
        to_schema: str,
    ) -> bool:
        from_table_name = f"{from_catalog}.{from_schema}.{from_table}"
        to_table_name = f"{to_catalog}.{to_schema}.{from_table}"
        try:
            self._create_alias_view(from_table_name, to_table_name)
            self._reapply_grants(from_table_name, to_table_name, target_view=True)
            return True
        except NotFound as err:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(err) or "[DELTA_TABLE_NOT_FOUND]" in str(err):
                logger.error(f"Could not find table {from_table_name}. Table not found.")
            else:
                logger.error(f"Failed to alias table {from_table_name}: {err!s}", exc_info=True)
            return False

    def _reapply_grants(self, from_table_name, to_table_name, *, target_view: bool = False):
        try:
            grants = self._ws.grants.get(SecurableType.TABLE, from_table_name)
        except NotFound:
            logger.warning(f"removed on the backend {from_table_name}")
            return
        if not grants.privilege_assignments:
            return
        logger.info(f"Applying grants on table {to_table_name}")
        grants_changes = []
        for permission in grants.privilege_assignments:
            if not permission.privileges:
                continue
            if not target_view:
                grants_changes.append(PermissionsChange(list(permission.privileges), permission.principal))
                continue
            privileges = set()
            for privilege in permission.privileges:
                if privilege != Privilege.MODIFY:
                    privileges.add(privilege)
            if privileges:
                grants_changes.append(PermissionsChange(list(privileges), permission.principal))

        self._ws.grants.update(SecurableType.TABLE, to_table_name, changes=grants_changes)

    def _recreate_table(self, from_table_name, to_table_name, *, is_managed: bool, del_table: bool):
        drop_table = f"DROP TABLE {escape_sql_identifier(from_table_name)}"
        if is_managed:
            create_table_sql = (
                f"CREATE TABLE IF NOT EXISTS {escape_sql_identifier(to_table_name)} "
                f"DEEP CLONE {escape_sql_identifier(from_table_name)}"
            )
            logger.info(f"Creating table {to_table_name}")
            self._execute(create_table_sql)
            if del_table:
                row_check = str(
                    next(
                        self._fetch(
                            f"SELECT (SELECT COUNT(*) FROM {escape_sql_identifier(to_table_name)})="
                            f"(SELECT COUNT(*) FROM {escape_sql_identifier(from_table_name)})"
                        )
                    )[0]
                )
                if row_check.lower() == "false":
                    logger.error(f"Number of rows did not match after cloning {from_table_name} to {to_table_name}.")
                    return
                logger.info(f"Dropping source table {from_table_name}")
                self._execute(drop_table)
        else:
            create_sql = str(next(self._fetch(f"SHOW CREATE TABLE {escape_sql_identifier(from_table_name)}"))[0])
            create_table_sql = create_sql.replace(
                f"CREATE TABLE {escape_sql_identifier(from_table_name)}",
                f"CREATE TABLE {escape_sql_identifier(to_table_name)}",
            )
            logger.info(f"Creating table {to_table_name}")
            self._execute(create_table_sql)
            logger.info(f"Dropping source table {from_table_name}")
            self._execute(drop_table)

    def _create_alias_view(self, from_table_name, to_table_name):
        create_view_sql = (
            f"CREATE VIEW {escape_sql_identifier(to_table_name)} "
            f"AS SELECT * FROM {escape_sql_identifier(from_table_name)}"
        )
        logger.info(f"Creating view {to_table_name} on {from_table_name}")
        self._execute(create_view_sql)

    def _move_view(
        self,
        from_catalog: str,
        from_schema: str,
        from_view: str,
        to_catalog: str,
        to_schema: str,
        *,
        del_view: bool,
        view_text: str | None = None,
    ) -> bool:
        from_view_name = f"{from_catalog}.{from_schema}.{from_view}"
        to_view_name = f"{to_catalog}.{to_schema}.{from_view}"
        try:
            self._recreate_view(to_view_name, view_text)
            self._reapply_grants(from_view_name, to_view_name)
            if del_view:
                logger.info(f"Dropping source view {from_view_name}")
                drop_sql = f"DROP VIEW {escape_sql_identifier(from_view_name)}"
                self._execute(drop_sql)
            return True
        except NotFound as err:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(err):
                logger.error(f"Could not find view {from_view_name}. View not found.")
            else:
                logger.error(f"Failed to move view {from_view_name}: {err!s}", exc_info=True)
        return False

    def _recreate_view(self, to_view_name, view_text):
        create_sql = f"CREATE VIEW {escape_sql_identifier(to_view_name)} AS {view_text}"
        logger.info(f"Creating view {to_view_name}")
        self._execute(create_sql)
