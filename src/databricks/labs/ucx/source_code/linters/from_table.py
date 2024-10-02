import logging
from collections.abc import Iterable

from sqlglot import parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import Table, Expression, Use
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    Deprecation,
    CurrentSessionState,
    SqlLinter,
    Fixer,
    UsedTable,
    TableSqlCollector,
)
from databricks.labs.ucx.source_code.sql.sql_parser import SqlExpression, SqlParser

logger = logging.getLogger(__name__)


class FromTableSqlLinter(SqlLinter, Fixer, TableSqlCollector):
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
        for info in SqlExpression(expression).collect_table_infos("hive_metastore", self._session_state):
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

    def collect_tables(self, source_code: str) -> Iterable[UsedTable]:
        try:
            yield from SqlParser.walk_expressions(
                source_code, lambda e: e.collect_table_infos("hive_metastore", self._session_state)
            )
        except SqlParseError as _:
            pass  # TODO establish a strategy

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
