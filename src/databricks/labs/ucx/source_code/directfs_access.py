from __future__ import annotations


import logging
from collections.abc import Sequence, Iterable
from dataclasses import dataclass, field
from datetime import datetime

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

logger = logging.getLogger(__name__)


@dataclass
class LineageAtom:

    object_type: str
    object_id: str
    other: dict[str, str] | None = None


@dataclass
class DirectFsAccess:
    """A record describing a Direct File System Access"""

    UNKNOWN = "unknown"

    path: str
    is_read: bool
    is_write: bool
    source_id: str = UNKNOWN
    source_timestamp: datetime = datetime.fromtimestamp(0)
    source_lineage: list[LineageAtom] = field(default_factory=list)
    job_id: int = -1
    job_name: str = UNKNOWN
    task_key: str = UNKNOWN
    assessment_start_timestamp: datetime = datetime.fromtimestamp(0)
    assessment_end_timestamp: datetime = datetime.fromtimestamp(0)

    def replace_source(
        self,
        source_id: str | None = None,
        source_lineage: list[LineageAtom] | None = None,
        source_timestamp: datetime | None = None,
    ):
        return DirectFsAccess(
            path=self.path,
            is_read=self.is_read,
            is_write=self.is_write,
            source_id=source_id or self.source_id,
            source_timestamp=source_timestamp or self.source_timestamp,
            source_lineage=source_lineage or self.source_lineage,
            job_id=self.job_id,
            job_name=self.job_name,
            task_key=self.task_key,
            assessment_start_timestamp=self.assessment_start_timestamp,
            assessment_end_timestamp=self.assessment_start_timestamp,
        )

    def replace_job_infos(
        self,
        job_id: int | None = None,
        job_name: str | None = None,
        task_key: str | None = None,
    ):
        return DirectFsAccess(
            path=self.path,
            is_read=self.is_read,
            is_write=self.is_write,
            source_id=self.source_id,
            source_timestamp=self.source_timestamp,
            source_lineage=self.source_lineage,
            job_id=job_id or self.job_id,
            job_name=job_name or self.job_name,
            task_key=task_key or self.task_key,
            assessment_start_timestamp=self.assessment_start_timestamp,
            assessment_end_timestamp=self.assessment_start_timestamp,
        )

    def replace_assessment_infos(
        self, assessment_start: datetime | None = None, assessment_end: datetime | None = None
    ):
        return DirectFsAccess(
            path=self.path,
            is_read=self.is_read,
            is_write=self.is_write,
            source_id=self.source_id,
            source_timestamp=self.source_timestamp,
            source_lineage=self.source_lineage,
            job_id=self.job_id,
            job_name=self.job_name,
            task_key=self.task_key,
            assessment_start_timestamp=assessment_start or self.assessment_start_timestamp,
            assessment_end_timestamp=assessment_end or self.assessment_start_timestamp,
        )


class _DirectFsAccessCrawler(CrawlerBase[DirectFsAccess]):

    def __init__(self, backend: SqlBackend, schema: str, table: str):
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, table, DirectFsAccess)

    def dump_all(self, dfsas: Sequence[DirectFsAccess]):
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
        sql = f"SELECT * FROM {self.full_name}"
        yield from self._backend.fetch(sql)

    def _crawl(self) -> Iterable[DirectFsAccess]:
        raise NotImplementedError()


class DirectFsAccessCrawlers:

    def __init__(self, sql_backend: SqlBackend, schema: str):
        self._sql_backend = sql_backend
        self._schema = schema

    def for_paths(self) -> _DirectFsAccessCrawler:
        return _DirectFsAccessCrawler(self._sql_backend, self._schema, "directfs_in_paths")

    def for_queries(self) -> _DirectFsAccessCrawler:
        return _DirectFsAccessCrawler(self._sql_backend, self._schema, "directfs_in_queries")
