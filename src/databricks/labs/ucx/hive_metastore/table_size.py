import logging
from dataclasses import dataclass
from functools import partial
from typing import Iterable

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)

@dataclass
class TableSize:
    catalog: str
    database: str
    name: str
    size_in_bytes: int


class TableSizeCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema):
        """
        Initializes a TablesSizeCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "table_size", TableSize)
        self._table_crawler = TablesCrawler(backend, schema)

    def _crawl(self) -> Iterable[TableSize]:
        """Crawls and lists tables using table crawler
        Identifies DBFS root tables and calculates the size for these.
        """
        table_sizes = []
        for table in self._table_crawler.snapshot():
            if not table.kind == "TABLE" or not table.is_dbfs_root():
                continue
            table_size = TableSize(catalog=table.catalog, database=table.database, name=table.name, size_in_bytes=0)
            table_sizes.append(table_size)
        return table_sizes

    def _try_load(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield Table(*row)

    def snapshot(self) -> list[TableSize]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(partial(self._try_load), partial(self._crawl))


