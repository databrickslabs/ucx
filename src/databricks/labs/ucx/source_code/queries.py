from collections.abc import Iterable

import logging
import sqlglot
from sqlglot.expressions import Table, Expression, Use
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Advice, Deprecation, Fixer, Linter

logger = logging.getLogger(__name__)


class FromTable(Linter, Fixer):
    def __init__(self, index: MigrationIndex, *, use_schema: str | None = None):
        self._index = index
        self._use_schema = use_schema

    def name(self) -> str:
        return 'table-migrate'

    @property
    def schema(self):
        return self._use_schema

    def lint(self, code: str) -> Iterable[Advice]:
        for statement in sqlglot.parse(code, dialect='databricks'):
            if not statement:
                continue
            if isinstance(statement, Use):
                table = statement.this
                db_name = table.this.this
                self._use_schema = db_name
                continue

            for table in statement.find_all(Table):
                catalog = self._catalog(table)
                if catalog != 'hive_metastore':
                    continue
                src_db = table.db if table.db else self._use_schema
                if not src_db:
                    logger.error(f"Could not determine schema for table {table.name}")
                    continue
                dst = self._index.get(src_db, table.name)
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

    def _catalog(self, table):
        if table.catalog:
            return table.catalog
        return self._use_schema if self._use_schema else 'hive_metastore'

    def apply(self, code: str) -> str:
        new_statements = []
        for statement in sqlglot.parse(code, read='databricks'):
            if not statement:
                continue
            if isinstance(statement, Use):
                table = statement.this
                db_name = table.this.this
                self._use_schema = db_name
                new_statements.append(statement.sql('databricks'))
                continue
            for old_table in self._dependent_tables(statement):
                src_db = old_table.db if old_table.db else self._use_schema
                if not src_db:
                    logger.error(f"Could not determine schema for table {old_table.name}")
                    continue
                dst = self._index.get(src_db, old_table.name)
                if not dst:
                    continue
                new_table = Table(catalog=dst.dst_catalog, db=dst.dst_schema, this=dst.dst_table)
                old_table.replace(new_table)
            new_sql = statement.sql('databricks')
            new_statements.append(new_sql)
        # TODO: Should the return preserve newlines?
        return '; '.join(new_statements)

    def _dependent_tables(self, statement: Expression):
        dependencies = []
        for old_table in statement.find_all(Table):
            dependencies.append(old_table)
        return dependencies
