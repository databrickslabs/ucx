import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.parallel import Threads

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import FasterTableScanCrawler, Table

logger = logging.getLogger(__name__)


@dataclass
class TableSize:
    catalog: str
    database: str
    name: str
    size_in_bytes: int


class TableSizeCrawler(CrawlerBase[TableSize]):
    # TODO: Ensure TablesCrawler and FasterTableScanCrawler share a common interface.
    def __init__(self, tables_crawler: TablesCrawler | FasterTableScanCrawler) -> None:
        """
        Initializes a TablesSizeCrawler instance.

        Args:
            tables_crawler (TablesCrawler): The crawler to use to obtain the table inventory.
        """
        # pylint: disable-next=import-error,import-outside-toplevel
        from pyspark.sql.session import SparkSession  # type: ignore[import-not-found]

        super().__init__(
            tables_crawler._backend,
            "hive_metastore",
            tables_crawler._schema,
            "table_size",
            TableSize,
        )
        self._tables_crawler = tables_crawler
        self._spark = SparkSession.builder.getOrCreate()

    def _crawl(self) -> Iterable[TableSize]:
        """Crawls and lists tables using table crawler
        Identifies DBFS root tables and calculates the size for these.
        """
        tasks = []
        for table in self._tables_crawler.snapshot():
            if not table.kind == "TABLE":
                continue
            if not table.is_dbfs_root:
                continue
            tasks.append(partial(self._safe_get_table_size, table))
        return Threads.strict('DBFS root table sizes', tasks)

    def _try_fetch(self) -> Iterable[TableSize]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield TableSize(*row)

    def _safe_get_table_size(self, table: Table) -> TableSize | None:
        logger.debug(f"Evaluating {table.key} table size.")
        try:
            # refresh table statistics to avoid stale stats in HMS
            self._backend.execute(f"ANALYZE table {table.safe_sql_key} compute STATISTICS NOSCAN")
            jvm_df = self._spark._jsparkSession.table(table.safe_sql_key)  # pylint: disable=protected-access
            size_in_bytes = jvm_df.queryExecution().analyzed().stats().sizeInBytes()
            return TableSize(
                catalog=table.catalog,
                database=table.database,
                name=table.name,
                size_in_bytes=size_in_bytes,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(e) or "[DELTA_TABLE_NOT_FOUND]" in str(e):
                logger.warning(f"Failed to evaluate {table.key} table size. Table not found.")
                return None
            if "[DELTA_INVALID_FORMAT]" in str(e):
                logger.warning(f"Unable to read Delta table {table.key}, please check table structure and try again.")
                return None
            if "[DELTA_MISSING_TRANSACTION_LOG]" in str(e):
                logger.warning(f"Delta table {table.key} is corrupt: missing transaction log.")
                return None
            logger.error(f"Failed to evaluate {table.key} table size: ", exc_info=True)

        return None
