import sqlglot
from sqlglot.expressions import Table as SqlTable

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migrate import Index
from databricks.labs.ucx.hive_metastore.tables import Table


class ViewToMigrate:

    table: Table
    table_dependencies: set[Table]
    view_dependencies: set[Table]

    def __init__(self, table: Table):
        assert table.view_text is not None
        self.table = table
        self.table_dependencies = set()
        self.view_dependencies = set()

    def compute_dependencies(self, all_tables: dict[str, Table]):
        if len(self.table_dependencies) + len(self.view_dependencies) == 0:
            assert self.table.view_text is not None
            statement = sqlglot.parse(self.table.view_text)[0]
            assert statement is not None
            for sql_table in statement.find_all(SqlTable):
                catalog = self._catalog(sql_table)
                if catalog != 'hive_metastore':
                    continue
                table_with_key = Table(catalog, sql_table.db, sql_table.name, "type", "")
                table = all_tables.get(table_with_key.key)
                assert table is not None
                if table.view_text is None:
                    self.table_dependencies.add(table)
                else:
                    self.view_dependencies.add(table)

    # duplicated from FromTable._catalog, not sure if it's worth factorizing
    @staticmethod
    def _catalog(table):
        if table.catalog:
            return table.catalog
        return 'hive_metastore'

    def __hash__(self):
        return hash(self.table)


class ViewsMigrator:

    def __init__(self, index: Index, crawler: TablesCrawler):
        self.index = index
        self.crawler = crawler
        self.result: list[ViewToMigrate] = []
        self.result_set: set[ViewToMigrate] = set()

    def sequence(self) -> list[Table]:
        table_values = self.crawler.snapshot()
        raw_tables = set(filter(lambda t: t.view_text is None, table_values))
        raw_views = set(table_values)
        raw_views.difference_update(raw_tables)
        table_keys = [table.key for table in table_values]
        all_tables = dict(zip(table_keys, table_values))
        views = {ViewToMigrate(view) for view in raw_views}
        while len(views) > 0:
            next_batch = self._next_batch(views, all_tables)
            self.result.extend(next_batch)
            self.result_set.update(next_batch)
            views.difference_update(next_batch)
        return [v.table for v in self.result]

    def _next_batch(self, views: set[ViewToMigrate], all_tables: dict[str, Table]) -> set[ViewToMigrate]:
        if len(views) == 0:
            return set()
        if len(views) == 1:
            return views
        result: set[ViewToMigrate] = set()
        for view in views:
            view.compute_dependencies(all_tables)
            not_batched_yet = list(filter(lambda v: v not in self.result_set, view.view_dependencies))
            if len(not_batched_yet) == 0:
                result.add(view)
        return result
