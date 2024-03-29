import sqlglot
from sqlglot import ParseError
from sqlglot.expressions import Expression as SqlExpression
from sqlglot.expressions import Table as SqlTable

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table


class ViewToMigrate:

    _view: Table
    _table_dependencies: set[Table]
    _view_dependencies: set[Table]

    def __init__(self, table: Table):
        if table.view_text is None:
            raise RuntimeError("Should never get there! A view must have 'view_text'!")
        self._view = table
        self._table_dependencies = set()
        self._view_dependencies = set()

    def compute_dependencies(self, all_tables: dict[str, Table]):
        if len(self._table_dependencies) + len(self._view_dependencies) > 0:
            return
        statement = self._parse_view_text()
        for sql_table in statement.find_all(SqlTable):
            catalog = self._catalog(sql_table)
            if catalog != 'hive_metastore':
                continue
            table_with_key = Table(catalog, sql_table.db, sql_table.name, "type", "")
            table = all_tables.get(table_with_key.key)
            if table is None:
                raise ValueError(
                    f"Unknown schema object: {table_with_key.key} in view SQL: {self._view.view_text} of table {self._view.key}"
                )
            if table.view_text is None:
                self._table_dependencies.add(table)
            else:
                self._view_dependencies.add(table)

    def _parse_view_text(self) -> SqlExpression:
        try:
            # below can never happen but avoids a pylint error
            assert self._view.view_text is not None
            statements = sqlglot.parse(self._view.view_text)
            if len(statements) != 1 or statements[0] is None:
                raise ValueError(f"Could not analyze view SQL: {self._view.view_text} of table {self._view.key}")
            return statements[0]
        except ParseError as e:
            raise ValueError(f"Could not analyze view SQL: {self._view.view_text} of table {self._view.key}") from e

    # duplicated from FromTable._catalog, not sure if it's worth factorizing
    @staticmethod
    def _catalog(table):
        if table.catalog:
            return table.catalog
        return 'hive_metastore'

    def __hash__(self):
        return hash(self._view)


class ViewsMigrator:

    def __init__(self, crawler: TablesCrawler):
        self._crawler = crawler
        self._result_view_list: list[ViewToMigrate] = []
        self._result_tables_set: set[Table] = set()

    def sequence(self) -> list[Table]:
        # sequencing is achieved using a very simple algorithm:
        # for each view, we register dependencies (extracted from view_text)
        # then given the remaining set of views to process,
        # and the growing set of views already processed
        # we check if each remaining view refers to not yet processed views
        # if none, then it's safe to add that view to the next batch of views
        # the complexity for a given set of views v and a dependency depth d looks like Ov^d
        # this seems enormous but in practice d remains small and v decreases rapidly
        table_list = self._crawler.snapshot()
        raw_tables = set(filter(lambda t: t.view_text is None, table_list))
        raw_views = set(table_list)
        raw_views.difference_update(raw_tables)
        table_keys = [table.key for table in table_list]
        all_tables = dict(zip(table_keys, table_list))
        views = {ViewToMigrate(view) for view in raw_views}
        while len(views) > 0:
            next_batch = self._next_batch(views, all_tables)
            self._result_view_list.extend(next_batch)
            self._result_tables_set.update([v._view for v in next_batch])
            views.difference_update(next_batch)
        return [v._view for v in self._result_view_list]

    def _next_batch(self, views: set[ViewToMigrate], all_tables: dict[str, Table]) -> set[ViewToMigrate]:
        # we can't (slightly) optimize by checking len(views) == 0 or 1,
        # because we'd lose the opportunity to check the SQL
        result: set[ViewToMigrate] = set()
        for view in views:
            view.compute_dependencies(all_tables)
            not_batched_yet = list(filter(lambda v: v not in self._result_tables_set, view._view_dependencies))
            if len(not_batched_yet) == 0:
                result.add(view)
        # prevent infinite loop
        if len(result) == 0 and len(views) > 0:
            raise ValueError(f"Circular view references are preventing migration: {views}")
        return result
