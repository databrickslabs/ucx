import sqlglot
from sqlglot.expressions import Table

from databricks.labs.ucx.code.base import Fixer
from databricks.labs.ucx.hive_metastore.table_migrate import Index


class FromTable(Fixer):
    def __init__(self, index: Index):
        self._index = index

    def match(self, query: str) -> bool:
        tables, matched = 0, 0
        for statement in sqlglot.parse(query):
            if not statement:
                continue
            for table in statement.find_all(Table):
                catalog = self._catalog(table)
                if catalog != 'hive_metastore':
                    continue
                tables += 1
                if not self._index.is_upgraded(table.db, table.name):
                    continue
                matched += 1
        return matched == tables

    def _key(self, table):
        catalog = self._catalog(table)
        if catalog != 'hive_metastore':
            return None
        return f"{catalog}.{table.db}.{table.name}".lower()

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
