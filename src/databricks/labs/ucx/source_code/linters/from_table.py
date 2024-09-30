import logging
from collections.abc import Iterable

from sqlglot import parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import Table, Expression, Use, Create, Drop
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import Deprecation, CurrentSessionState, SqlLinter, Fixer, Failure, TableInfo

logger = logging.getLogger(__name__)


class FromTableSqlLinter(SqlLinter, Fixer):
    """Linter and Fixer for table migrations in SQL queries.

    This class is responsible for identifying and fixing table migrations in
    SQL queries.
    """

    def __init__(self, index: TableMigrationIndex, session_state: CurrentSessionState):
        """
        Initializes the FromTableLinter class.

        Args:
            index: The migration index, which is a mapping of source tables to destination tables.
            session_state: The current session state, which will be used to track the current schema.

        We need to be careful with the nomenclature here. For instance when parsing a table reference,
        sqlglot uses `db` instead of `schema` to refer to the schema. The following table references
        show how sqlglot represents them:::

                catalog.schema.table    -> Table(catalog='catalog', db='schema', this='table')
                schema.table                 -> Table(catalog='', db='schema', this='table')
                table                               -> Table(catalog='', db='', this='table')
        """
        self._index: TableMigrationIndex = index
        self._session_state: CurrentSessionState = session_state

    @property
    def name(self) -> str:
        return 'table-migrate'

    @property
    def schema(self):
        return self._session_state.schema

    def lint_expression(self, expression: Expression):
        for table in expression.find_all(Table):
            try:
                yield from self._unsafe_lint_expression(expression, table)
            except Exception as _:  # pylint: disable=broad-exception-caught
                yield Failure(
                    code='sql-parse-error',
                    message=f"Could not parse SQL expression: {expression} ",
                    # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=1024,
                )

    def _unsafe_lint_expression(self, expression: Expression, table: Table):
        info = self._collect_table_info(expression, table)
        if not info:
            return
        dst = self._index.get(info.schema_name, info.table_name)
        if not dst:
            return
        yield Deprecation(
            code='table-migrated-to-uc',
            message=f"Table {info.schema_name}.{info.table_name} is migrated to {dst.destination()} in Unity Catalog",
            # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        )

    def collect_legacy_table_infos(self, sql_code: str) -> Iterable[TableInfo]:
        try:
            expressions = parse_sql(sql_code, read='databricks')
            for expression in expressions:
                if not expression:
                    continue
                yield from self._collect_table_infos(expression)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {sql_code}", exc_info=e)
            yield self.sql_parse_failure(sql_code)

    def _collect_table_infos(self, expression: Expression) -> Iterable[TableInfo]:
        for table in expression.find_all(Table):
            info = self._collect_table_info(expression, table)
            if info is None:
                continue
            yield info

    def _collect_table_info(self, expression: Expression, table: Table) -> TableInfo | None:
        if isinstance(expression, Use):
            # Sqlglot captures the database name in the Use statement as a Table, with
            # the schema  as the table name.
            self._session_state.schema = table.name
            return None
        if isinstance(expression, Drop) and getattr(expression, "kind", None) == "SCHEMA":
            # Sqlglot captures the schema name in the Drop statement as a Table, with
            # the schema  as the db name.
            return None
        if isinstance(expression, Create) and getattr(expression, "kind", None) == "SCHEMA":
            # Sqlglot captures the schema name in the Create statement as a Table, with
            # the schema  as the db name.
            self._session_state.schema = table.db
            return None

        # we only migrate tables in the hive_metastore catalog
        catalog_name = self._catalog(table)
        if catalog_name != 'hive_metastore':
            return None
        # Sqlglot uses db instead of schema, watch out for that
        src_schema = table.db if table.db else self._session_state.schema
        if not src_schema:
            logger.error(f"Could not determine schema for table {table.name}")
            return None
        return TableInfo(catalog_name=catalog_name,
                         schema_name=src_schema,
                         table_name=table.name,
        )

    @staticmethod
    def _catalog(table):
        if table.catalog:
            return table.catalog
        return 'hive_metastore'

    def apply(self, code: str) -> str:
        new_statements = []
        for statement in parse_sql(code, read='databricks'):
            if not statement:
                continue
            if isinstance(statement, Use):
                table = statement.this
                self._session_state.schema = table.name
                new_statements.append(statement.sql('databricks'))
                continue
            for old_table in self._dependent_tables(statement):
                src_schema = old_table.db if old_table.db else self._session_state.schema
                if not src_schema:
                    logger.error(f"Could not determine schema for table {old_table.name}")
                    continue
                dst = self._index.get(src_schema, old_table.name)
                if not dst:
                    continue
                new_table = Table(catalog=dst.dst_catalog, db=dst.dst_schema, this=dst.dst_table, alias=old_table.alias)
                old_table.replace(new_table)
            new_sql = statement.sql('databricks')
            new_statements.append(new_sql)
        return '; '.join(new_statements)

    @classmethod
    def _dependent_tables(cls, statement: Expression):
        dependencies = []
        for old_table in statement.find_all(Table):
            catalog = cls._catalog(old_table)
            if catalog != 'hive_metastore':
                continue
            dependencies.append(old_table)
        return dependencies
