from collections.abc import Sequence

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.source_code.base import DFSA
from databricks.labs.lsql.backends import SqlBackend


class DfsaCrawler(CrawlerBase):

    def __init__(self, backend: SqlBackend, schema: str):
        """
        Initializes a DFSACrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "direct_file_system_access", DFSA)

    def append(self, dfsas: Sequence[DFSA]):
        self._append_records(dfsas)
