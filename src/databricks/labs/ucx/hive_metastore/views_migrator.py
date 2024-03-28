from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus
from databricks.labs.ucx.hive_metastore.tables import Table


class ViewsMigrator:

    def __init__(self, index: Index, crawler: TablesCrawler):
        self.index = index
        self.crawler = crawler

    def sequence(self) -> list[MigrationStatus]:
        views = self._list_views()
        if len(views) == 0:
            return []
        if len(views) == 1:
            view = views[0]
            migration = self.index.get(view.database, view.name)
            return [] if migration is None else [migration]
        # TODO
        raise Exception("Not implemented yet!")

    def _list_views(self) -> list[Table]:
        tables = self.crawler.snapshot()
        return list(filter(lambda t: t.view_text is not None, tables))
