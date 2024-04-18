from collections.abc import Iterable

import logging
import sqlglot
from sqlglot.expressions import Table, Expression, Use
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Advice, Deprecation, Fixer, Linter

logger = logging.getLogger(__name__)


class FromTable(Linter, Fixer):
    """Linter and Fixer for table migrations in SQL queries.

    This class is responsible for identifying and fixing table migrations in
    SQL queries.
    """

    def __init__(self, index: MigrationIndex, *, schema: str | None = None):
        """
        Initializes the FromTable class.

        Args:
            index: The migration index, which is a mapping of source tables to destination tables.
            schema (str or None): The schema that the tables belong to by default, which defaults to `None`.

        We need to be careful with the nomenclature here. For instance when parsing a table reference,
        sqlglot uses `db` instead of `schema` to refer to the schema. The following table references
        show how sqlglot represents them:::

                catalog.schema.table    -> Table(catalog='catalog', db='schema', this='table')
                schema.table                 -> Table(catalog='', db='schema', this='table')
                table                               -> Table(catalog='', db='', this='table')
        """
        self._index = index
        self._schema = schema

    def name(self) -> str:
        return 'table-migrate'

    @property
    def schema(self):
        return self._schema

    def lint(self, code: str, schema: str | None = None) -> Iterable[Advice]:
        # Allow the lint to override the schema because a previous lint of say a notebook cell
        # may have set the schema.
        if schema:
            self._schema = schema
        for statement in sqlglot.parse(code, read='databricks'):
            if not statement:
                continue
            for table in statement.find_all(Table):
                if isinstance(statement, Use):
                    # Sqlglot captures the database name in the Use statement as a Table, with
                    # the schema  as the table name.
                    self._schema = table.name
                    continue

                # we only migrate tables in the hive_metastore catalog
                if self._catalog(table) != 'hive_metastore':
                    continue
                # Sqlglot uses db instead of schema, watch out for that
                src_schema = table.db if table.db else self._schema
                if not src_schema:
                    logger.error(f"Could not determine schema for table {table.name}")
                    continue
                dst = self._index.get(src_schema, table.name)
                if not dst:
                    continue
                yield Deprecation(
                    code='table-migrate',
                    message=f"Table {table.db}.{table.name} is migrated to {dst.destination()} in Unity Catalog",
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

    def apply(self, code: str, schema: str | None = None) -> str:
        # Allow the apply to override the schema because a previous lint of say a notebook cell
        # may have set the schema.If not set, then schema is not overwritten which does allow
        # reuse potentially.
        if schema:
            self._schema = schema
        new_statements = []
        for statement in sqlglot.parse(code, read='databricks'):
            if not statement:
                continue
            if isinstance(statement, Use):
                table = statement.this
                self._schema = table.name
                new_statements.append(statement.sql('databricks'))
                continue
            for old_table in self._dependent_tables(statement):
                src_schema = old_table.db if old_table.db else self._schema
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
