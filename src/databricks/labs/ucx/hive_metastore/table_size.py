import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

from databricks.labs.ucx.framework.crawlers import CrawlerBase, RuntimeBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler

logger = logging.getLogger(__name__)


@dataclass
class TableSize:
    catalog: str
    database: str
    name: str
    size_in_bytes: int


class TableSizeCrawler(CrawlerBase):
    def __init__(self, backend: RuntimeBackend, schema):
        """
        Initializes a TablesSizeCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        self._backend: RuntimeBackend = backend
        super().__init__(backend, "hive_metastore", schema, "table_size", TableSize)
        self._tables_crawler = TablesCrawler(backend, schema)

    def _crawl(self) -> Iterable[TableSize]:
        """Crawls and lists tables using table crawler
        Identifies DBFS root tables and calculates the size for these.
        """
        for table in self._tables_crawler.snapshot():
            if not table.kind == "TABLE":
                continue
            if not table.is_dbfs_root():
                continue
            size_in_bytes = self._backend.get_table_size(table.key)
            table_size = TableSize(
                catalog=table.catalog, database=table.database, name=table.name, size_in_bytes=size_in_bytes
            )
            yield table_size

    def _try_load(self) -> Iterable[TableSize]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield TableSize(*row)

    def snapshot(self) -> list[TableSize]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(partial(self._try_load), partial(self._crawl))
