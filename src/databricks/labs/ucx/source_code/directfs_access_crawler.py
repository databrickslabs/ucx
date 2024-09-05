import logging
from collections.abc import Sequence, Iterable

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.source_code.base import DirectFsAccess

logger = logging.getLogger(__name__)


class DirectFsAccessCrawler(CrawlerBase):

    def __init__(self, backend: SqlBackend, schema: str):
        """
        Initializes a DFSACrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "direct_file_system_access", DirectFsAccess)

    def append(self, dfsas: Sequence[DirectFsAccess]):
        try:
            self._append_records(dfsas)
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def snapshot(self) -> Iterable[DirectFsAccess]:
        sql = f"SELECT * FROM {self.full_name}"
        yield from self._backend.fetch(sql)
