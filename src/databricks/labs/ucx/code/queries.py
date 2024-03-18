from typing import Iterable

import sqlglot
from sqlglot.expressions import Table

from databricks.labs.ucx.code.base import Fixer, Diagnostic, Position, Range, Severity, Analyser, DiagnosticTag

from databricks.labs.ucx.hive_metastore.table_migrate import Index


class FromTable(Analyser, Fixer):
    def __init__(self, index: Index):
        self._index = index

    def analyse(self, query: str) -> Iterable[Diagnostic]:
        for statement in sqlglot.parse(query):
            if not statement:
                continue
            for table in statement.find_all(Table):
                catalog = self._catalog(table)
                if catalog != 'hive_metastore':
                    continue
                if not self._index.is_upgraded(table.db, table.name):
                    continue
                dst = self._index.get(table.db, table.name)
                yield Diagnostic(
                    # see https://github.com/tobymao/sqlglot/issues/3159
                    range=Range.make(0, 0, 0, 1024),
                    code='table-migrate',
                    source='databricks.labs.ucx',
                    message=f"Table {table.db}.{table.name} is migrated to {dst.destination()} in Unity Catalog",
                    severity=Severity.ERROR,
                    tags=[DiagnosticTag.DEPRECATED],
                )

    @staticmethod
    def _catalog(table):
        catalog = table.catalog
        if not catalog:
            catalog = 'hive_metastore'
        return catalog

    def apply(self, query: str) -> str:
        new_statements = []
        for statement in sqlglot.parse(query):
            if not statement:
                continue
            for old_table in statement.find_all(Table):
                catalog = self._catalog(old_table)
                if catalog != 'hive_metastore':
                    continue
                dst = self._index.get(old_table.db, old_table.name)
                if not dst:
                    continue
                new_table = Table(catalog=dst.dst_catalog, db=dst.dst_schema, this=dst.dst_table)
                old_table.replace(new_table)
            new_sql = statement.sql('databricks')
            new_statements.append(new_sql)
        return '; '.join(new_statements)
