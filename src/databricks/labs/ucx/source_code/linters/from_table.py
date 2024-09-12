import logging
from sqlglot import parse as parse_sql
from sqlglot.expressions import Table, Expression, Use, Create
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import Deprecation, CurrentSessionState, SqlLinter, Fixer

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
            if isinstance(expression, Use):
                # Sqlglot captures the database name in the Use statement as a Table, with
                # the schema  as the table name.
                self._session_state.schema = table.name
                continue
            if isinstance(expression, Create) and getattr(expression, "kind", None) == "SCHEMA":
                # Sqlglot captures the schema name in the Create statement as a Table, with
                # the schema  as the db name.
                self._session_state.schema = table.db
                continue

            # we only migrate tables in the hive_metastore catalog
            if self._catalog(table) != 'hive_metastore':
                continue
            # Sqlglot uses db instead of schema, watch out for that
            src_schema = table.db if table.db else self._session_state.schema
            if not src_schema:
                logger.error(f"Could not determine schema for table {table.name}")
                continue
            dst = self._index.get(src_schema, table.name)
            if not dst:
                continue
            yield Deprecation(
                code='table-migrated-to-uc',
                message=f"Table {src_schema}.{table.name} is migrated to {dst.destination()} in Unity Catalog",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
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
                new_table = Table(catalog=dst.dst_catalog, db=dst.dst_schema, this=dst.dst_table)
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
