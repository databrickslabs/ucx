from collections.abc import Iterable

import sqlglot
from sqlglot.expressions import Table, Expression

import databricks.labs.ucx.hive_metastore.tables
from databricks.labs.ucx.hive_metastore.table_migrate import MigrationIndex
from databricks.labs.ucx.source_code.base import Advice, Deprecation, Fixer, Linter


class FromTable(Linter, Fixer):
    def __init__(self, index: MigrationIndex):
        self._index = index

    def name(self) -> str:
        return 'table-migrate'

    def lint(self, code: str) -> Iterable[Advice]:
        for statement in sqlglot.parse(code):
            if not statement:
                continue
            for table in statement.find_all(Table):
                catalog = self._catalog(table)
                if catalog != 'hive_metastore':
                    continue
                dst = self._index.get(table.db, table.name)
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

    def apply(self, code: str, *, use_schema: str = "") -> str:
        new_statements = []
        for statement in self.get_statements(code):
            if not statement:
                continue
            for old_table in self.get_dependencies(statement, use_schema=use_schema):
                catalog = self._catalog(old_table)
                dst = self._index.get(old_table.database, old_table.name)
                if not dst:
                    continue
                new_table = Table(catalog=dst.dst_catalog, db=dst.dst_schema, this=dst.dst_table)
                old_table.replace(new_table)
            new_sql = statement.sql('databricks')
            new_statements.append(new_sql)
        return '; '.join(new_statements)

    @staticmethod
    def get_statements(code: str):
        return sqlglot.parse(code)

    @classmethod
    def get_dependencies(cls, statement: Expression, *, use_schema: str = ""):
        dependencies = []
        for old_table in statement.find_all(Table):
            catalog = cls._catalog(old_table)
            if catalog != 'hive_metastore':
                continue
            src_db = old_table.db if old_table.db else use_schema
            dependencies.append(Table(catalog=catalog, db=src_db, name=old_table.name))
        return dependencies

    @classmethod
    def get_dependencies_as_tables(cls, statement: Expression, *, use_schema: str = ""):
        for table in cls.get_dependencies(statement, use_schema=use_schema):
            yield databricks.labs.ucx.hive_metastore.tables.Table(table.catalog,
                                                                  table.db,
                                                                  table.name, "type", "")

