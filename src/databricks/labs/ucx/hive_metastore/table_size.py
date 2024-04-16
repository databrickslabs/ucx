import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.hive_metastore import TablesCrawler

logger = logging.getLogger(__name__)


@dataclass
class TableSize:
    catalog: str
    database: str
    name: str
    size_in_bytes: int


class TableSizeCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema, include_databases: list[str] | None = None):
        """
        Initializes a TablesSizeCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        # pylint: disable-next=import-error,import-outside-toplevel
        from pyspark.sql.session import SparkSession  # type: ignore[import-not-found]

        self._backend = backend
        super().__init__(backend, "hive_metastore", schema, "table_size", TableSize)
        self._tables_crawler = TablesCrawler(backend, schema, include_databases)
        self._spark = SparkSession.builder.getOrCreate()

    def _crawl(self) -> Iterable[TableSize]:
        """Crawls and lists tables using table crawler
        Identifies DBFS root tables and calculates the size for these.
        """
        for table in self._tables_crawler.snapshot():
            if not table.kind == "TABLE":
                continue
            if not table.is_dbfs_root:
                continue
            size_in_bytes = self._safe_get_table_size(table.key)
            if size_in_bytes is None:
                continue  # table does not exist anymore or is corrupted

            yield TableSize(
                catalog=table.catalog, database=table.database, name=table.name, size_in_bytes=size_in_bytes
            )

    def _try_load(self) -> Iterable[TableSize]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {self.full_name}"):
            yield TableSize(*row)

    def snapshot(self) -> list[TableSize]:
        """
        Takes a snapshot of tables in the specified catalog and database.
        Return None if the table cannot be found anymore.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(partial(self._try_load), partial(self._crawl))

    def _safe_get_table_size(self, table_full_name: str) -> int | None:
        logger.debug(f"Evaluating {table_full_name} table size.")
        try:
            # pylint: disable-next=protected-access
            return self._spark._jsparkSession.table(table_full_name).queryExecution().analyzed().stats().sizeInBytes()
        except Exception as e:  # pylint: disable=broad-exception-caught
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(e) or "[DELTA_TABLE_NOT_FOUND]" in str(e):
                logger.warning(f"Failed to evaluate {table_full_name} table size. Table not found.")
                return None
            if "[DELTA_MISSING_TRANSACTION_LOG]" in str(e):
                logger.warning(f"Delta table {table_full_name} is corrupted: missing transaction log.")
                return None
            logger.error(f"Failed to evaluate {table_full_name} table size: ", exc_info=True)

        return None
