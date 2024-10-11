from __future__ import annotations

import logging
from collections.abc import Sequence, Iterable

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.source_code.base import DirectFsAccess

logger = logging.getLogger(__name__)


class DirectFsAccessCrawler(CrawlerBase[DirectFsAccess]):

    @classmethod
    def for_paths(cls, backend: SqlBackend, schema) -> DirectFsAccessCrawler:
        return DirectFsAccessCrawler(backend, schema, "directfs_in_paths")

    @classmethod
    def for_queries(cls, backend: SqlBackend, schema) -> DirectFsAccessCrawler:
        return DirectFsAccessCrawler(backend, schema, "directfs_in_queries")

    def __init__(self, backend: SqlBackend, schema: str, table: str):
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend=backend, catalog="hive_metastore", schema=schema, table=table, klass=DirectFsAccess)

    def dump_all(self, dfsas: Sequence[DirectFsAccess]) -> None:
        """This crawler doesn't follow the pull model because the fetcher fetches data for 2 crawlers, not just one
        It's not **bad** because all records are pushed at once.
        Providing a multi-entity crawler is out-of-scope of this PR
        """
        try:
            # TODO until we historize data, we append all DFSAs
            self._update_snapshot(dfsas, mode="append")
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def _try_fetch(self) -> Iterable[DirectFsAccess]:
        sql = f"SELECT * FROM {escape_sql_identifier(self.full_name)}"
        for row in self._backend.fetch(sql):
            yield self._klass.from_dict(row.as_dict())

    def _crawl(self) -> Iterable[DirectFsAccess]:
        return []
        # TODO raise NotImplementedError() once CrawlerBase supports empty snapshots


class DirectFsAccessOwnership(Ownership[DirectFsAccess]):
    """Determine ownership of records reporting direct filesystem access.

    This is intended to be:

     - For queries, the creator of the query (if known).
     - For jobs, the owner of the path for the notebook or source (if known).

    At present this information is not gathered during the crawling process, so it can't be reported here.
    """

    def _maybe_direct_owner(self, record: DirectFsAccess) -> None:
        # TODO: Implement this once the creator/ownership information is exposed during crawling.
        return None
