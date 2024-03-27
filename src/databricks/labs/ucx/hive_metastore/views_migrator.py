from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migrate import Index
from databricks.labs.ucx.hive_metastore.tables import Table


class ViewsMigrator:

    def __init__(self, index: Index, crawler: TablesCrawler):
        self.index = index
        self.crawler = crawler

    def sequence(self) -> list[Table]:
        return []
