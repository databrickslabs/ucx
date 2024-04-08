from collections.abc import Iterable
from collections.abc import Callable

import sqlglot
from sqlglot import ParseError
from sqlglot.expressions import Table, Expression

import databricks.labs.ucx.hive_metastore.tables as ucx_tables
from databricks.labs.ucx.source_code.base import Advice, Deprecation, Fixer, Linter


class FromTable(Linter, Fixer):
    def __init__(self, dst_lookup, *, use_schema: str = ""):
        self._dst_lookup: Callable[[str, str], ucx_tables.Table | None] = dst_lookup
        self._use_schema = use_schema

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
                dst = self._dst_lookup(table.db, table.name)
                if not dst:
                    continue
                yield Deprecation(
                    code='table-migrate',
                    message=f"Table {table.db}.{table.name} is migrated to {dst.key} in Unity Catalog",
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
        for statement in self.get_statements(code):
            if not statement:
                continue
            for old_table in self.get_dependencies(statement):
                src_db = old_table.db if old_table.db else self._use_schema
                dst = self._dst_lookup(src_db, old_table.name)
                if not dst:
                    continue
                new_table = Table(catalog=dst.catalog, db=dst.database, this=dst.name)
                old_table.replace(new_table)
            new_sql = statement.sql('databricks')
            new_statements.append(new_sql)
        return '; '.join(new_statements)

    @staticmethod
    def get_statements(code: str):
        return sqlglot.parse(code)

    @classmethod
    def get_dependencies(cls, statement: Expression):
        dependencies = []
        for old_table in statement.find_all(Table):
            catalog = cls._catalog(old_table)
            if catalog != 'hive_metastore':
                continue
            dependencies.append(old_table)
        return dependencies

    @classmethod
    # This method is used to get the dependencies for a query as a list of tables
    def get_view_sql_dependencies(cls, code: str, *, use_schema: str = ""):
        try:
            statements = sqlglot.parse(code)
            if len(statements) != 1 or statements[0] is None:
                raise ValueError(f"Could not analyze view SQL: {code}")
        except ParseError as e:
            raise ValueError(f"Could not analyze view SQL: {code}") from e
        statement = statements[0]
        for table in cls.get_dependencies(statement):
            src_db = table.db if table.db else use_schema
            yield ucx_tables.Table("hive_metastore", src_db, table.name, "type", "")
