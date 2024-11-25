from __future__ import annotations

import logging
from collections.abc import Sequence, Iterable

from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError, NotFound

from databricks.labs.ucx.framework.owners import (
    Ownership,
    AdministratorLocator,
    WorkspacePathOwnership,
    LegacyQueryOwnership,
)
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

    def __init__(self, sql_backend: SqlBackend, schema: str, table: str):
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(
            sql_backend=sql_backend, catalog="hive_metastore", schema=schema, table=table, klass=DirectFsAccess
        )

    def dump_all(self, dfsas: Sequence[DirectFsAccess]) -> None:
        """This crawler doesn't follow the pull model because the fetcher fetches data for 2 crawlers, not just one
        It's not **bad** because all records are pushed at once.
        Providing a multi-entity crawler is out-of-scope of this PR
        """
        try:
            # TODO: Until we historize data, we append all DFSAs
            # UPDATE: We historize DFSA from WorkflowLinter, not from QueryLinter yet
            self._update_snapshot(dfsas, mode="append")
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def _try_fetch(self) -> Iterable[DirectFsAccess]:
        sql = f"SELECT * FROM {escape_sql_identifier(self.full_name)}"
        for row in self._sql_backend.fetch(sql):
            yield self._klass.from_dict(row.asDict())

    def _crawl(self) -> Iterable[DirectFsAccess]:
        return []
        # TODO raise NotImplementedError() once CrawlerBase supports empty snapshots


class DirectFsAccessOwnership(Ownership[DirectFsAccess]):
    """Determine ownership of records reporting direct filesystem access.

    This is intended to be:

     - For queries, the creator of the query (if known).
     - For jobs, the owner of the path for the notebook or source (if known).
    """

    def __init__(
        self,
        administrator_locator: AdministratorLocator,
        workspace_path_ownership: WorkspacePathOwnership,
        legacy_query_ownership: LegacyQueryOwnership,
        workspace_client: WorkspaceClient,
    ) -> None:
        super().__init__(administrator_locator)
        self._workspace_path_ownership = workspace_path_ownership
        self._legacy_query_ownership = legacy_query_ownership
        self._workspace_client = workspace_client

    def _maybe_direct_owner(self, record: DirectFsAccess) -> str | None:
        if record.source_type == 'QUERY' and record.query_id:
            return self._legacy_query_ownership.owner_of(record.query_id)
        if record.source_type in {'NOTEBOOK', 'FILE'}:
            return self._notebook_owner(record)
        logger.warning(f"Unknown source type {record.source_type} for {record.source_id}")
        return None

    def _notebook_owner(self, record):
        try:
            workspace_path = WorkspacePath(self._workspace_client, record.source_id)
            owner = self._workspace_path_ownership.owner_of(workspace_path)
            return owner
        except NotFound:
            return None
