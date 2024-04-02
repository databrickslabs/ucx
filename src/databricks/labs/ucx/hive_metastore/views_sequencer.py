from collections.abc import Collection
from dataclasses import dataclass

import sqlglot
from sqlglot import ParseError
from sqlglot.expressions import Expression as SqlExpression
from sqlglot.expressions import Table as SqlTable

from databricks.labs.ucx.hive_metastore.mapping import TableToMigrate
from databricks.labs.ucx.hive_metastore.tables import Table, What


@dataclass
class ViewToMigrate(TableToMigrate):

    _table_dependencies: list[TableToMigrate] | None = None
    _view_dependencies: list[TableToMigrate] | None = None

    def __post_init__(self):
        if self.src.view_text is None:
            raise RuntimeError("Should never get there! A view must have 'view_text'!")

    def table_dependencies(self, all_tables: dict[str, TableToMigrate]) -> list[TableToMigrate]:
        if self._table_dependencies is None or self._view_dependencies is None:
            self._compute_dependencies(all_tables)
        assert self._table_dependencies is not None
        return self._table_dependencies

    def view_dependencies(self, all_tables: dict[str, TableToMigrate]) -> list[TableToMigrate]:
        if self._table_dependencies is None or self._view_dependencies is None:
            self._compute_dependencies(all_tables)
        assert self._view_dependencies is not None
        return self._view_dependencies

    def _compute_dependencies(self, all_tables: dict[str, TableToMigrate]):
        table_dependencies = set()
        view_dependencies = set()
        statement = self._parse_view_text()
        for sql_table in statement.find_all(SqlTable):
            catalog = self._catalog(sql_table)
            if catalog != 'hive_metastore':
                continue
            table_with_key = Table(catalog, sql_table.db, sql_table.name, "type", "")
            table = all_tables.get(table_with_key.key)
            if table is None:
                raise ValueError(
                    f"Unknown schema object: {table_with_key.key} in view SQL: {self.src.view_text} of table {self.src.key}"
                )
            if table.src.view_text is None:
                table_dependencies.add(table)
            else:
                view_dependencies.add(table)
        self._table_dependencies = list(table_dependencies)
        self._view_dependencies = list(view_dependencies)

    def _parse_view_text(self) -> SqlExpression:
        try:
            # below can never happen but avoids a pylint error
            assert self.src.view_text is not None
            statements = sqlglot.parse(self.src.view_text)
            if len(statements) != 1 or statements[0] is None:
                raise ValueError(
                    f"Could not analyze view SQL: {self.src.view_text} of table {self.src.key}"
                )
            return statements[0]
        except ParseError as e:
            raise ValueError(
                f"Could not analyze view SQL: {self.src.view_text} of table {self.src.key}"
            ) from e

    # duplicated from FromTable._catalog, not sure if it's worth factorizing
    @staticmethod
    def _catalog(table):
        if table.catalog:
            return table.catalog
        return 'hive_metastore'

    def __hash__(self):
        return hash(self.src)

    def __eq__(self, other):
        return isinstance(other, TableToMigrate) and self.src == other.src


class TableMigrationSequencer:

    def __init__(self, tables: Collection[TableToMigrate]):
        self._tables = tables
        self._result_view_list: list[ViewToMigrate] = []
        self._result_tables_set: set[TableToMigrate] = set()

    def sequence_batches(self, what: What | None) -> list[list[TableToMigrate]]:
        # sequencing is achieved using a very simple algorithm:
        # for each view, we register dependencies (extracted from view_text)
        # then given the remaining set of views to process,
        # and the growing set of views already processed
        # we check if each remaining view refers to not yet processed views
        # if none, then it's safe to add that view to the next batch of views
        # the complexity for a given set of views v and a dependency depth d looks like Ov^d
        # this seems enormous but in practice d remains small and v decreases rapidly
        all_tables = {}
        views = set()
        tables = []
        for table in self._tables:
            all_tables[table.src.key] = table
            # when migrating views we want all tables/views
            if what == What.VIEW:
                if table.src.view_text is None:
                    tables.append(table)
                else:
                    views.add(ViewToMigrate(table.src, table.rule))
            else:
                # when migrating tables we only want specific tables
                if what is not None and table.src.what != what:
                    continue
                tables.append(table)
        # when migrating tables we only want tables in 1 batch
        if what != What.VIEW:
            return [tables]
        # when migrating views we only want views in n batches
        batches = []
        while len(views) > 0:
            next_batch = self._next_batch(views, all_tables)
            self._result_view_list.extend(next_batch)
            self._result_tables_set.update([v for v in next_batch])
            views.difference_update(next_batch)
            batches.append(list(v for v in next_batch))
        return batches

    def _next_batch(self, views: set[ViewToMigrate], all_tables: dict[str, TableToMigrate]) -> set[ViewToMigrate]:
        # we can't (slightly) optimize by checking len(views) == 0 or 1,
        # because we'd lose the opportunity to check the SQL
        result: set[ViewToMigrate] = set()
        for view in views:
            view_deps = view.view_dependencies(all_tables)
            if len(view_deps) == 0:
                result.add(view)
            else:
                # does the view have at least one view dependency that is not yet processed ?
                not_processed_yet = next((t for t in view_deps if t not in self._result_tables_set), None)
                if not_processed_yet is None:
                    result.add(view)
        # prevent infinite loop
        if len(result) == 0 and len(views) > 0:
            raise ValueError(f"Circular view references are preventing migration: {views}")
        return result
