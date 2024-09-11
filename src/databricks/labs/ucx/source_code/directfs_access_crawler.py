import logging
from collections.abc import Sequence, Iterable
from typing import TypeVar

from databricks.labs.ucx.framework.crawlers import CrawlerBase, Result
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.source_code.base import DirectFsAccessInPath, DirectFsAccessInQuery, AbstractDirectFsAccess

logger = logging.getLogger(__name__)


T = TypeVar("T", bound=AbstractDirectFsAccess)


class _DirectFsAccessCrawler(CrawlerBase):

    def __init__(self, backend: SqlBackend, schema: str, table: str, klass: type):
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, table, klass)

    def append(self, dfsas: Sequence[T]):
        try:
            self._append_records(dfsas)
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def _try_fetch(self) -> Iterable[T]:
        sql = f"SELECT * FROM {self.full_name}"
        yield from self._backend.fetch(sql)

    def _crawl(self) -> Iterable[Result]:
        return []


class DirectFsAccessCrawlers:

    def __init__(self, sql_backend: SqlBackend, schema: str):
        self._sql_backend = sql_backend
        self._schema = schema

    def for_paths(self) -> _DirectFsAccessCrawler:
        return _DirectFsAccessCrawler(
            self._sql_backend, self._schema, "direct_file_system_access_in_paths", DirectFsAccessInPath
        )

    def for_queries(self) -> _DirectFsAccessCrawler:
        return _DirectFsAccessCrawler(
            self._sql_backend, self._schema, "direct_file_system_access_in_queries", DirectFsAccessInQuery
        )
