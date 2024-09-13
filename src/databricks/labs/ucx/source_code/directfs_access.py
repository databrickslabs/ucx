from __future__ import annotations


import logging
from collections.abc import Sequence, Iterable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, TypeVar

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.framework.utils import escape_sql_identifier

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
    assessment_start_timestamp: datetime = datetime.fromtimestamp(0)
    assessment_end_timestamp: datetime = datetime.fromtimestamp(0)

    def _as_dict(self):
        result = asdict(self)
        result["source_lineage"] = self.source_lineage
        return result

    def from_dict(self, data: dict[str, Any]):
        return DirectFsAccess(**data)

    def replace_source(
        self,
        source_id: str | None = None,
        source_lineage: list[LineageAtom] | None = None,
        source_timestamp: datetime | None = None,
    ):
        data = {
            **self._as_dict(),
            "source_id": source_id or self.source_id,
            "source_timestamp": source_timestamp or self.source_timestamp,
            "source_lineage": source_lineage or self.source_lineage,
        }
        return self.from_dict(data)

    def replace_assessment_infos(
        self, assessment_start: datetime | None = None, assessment_end: datetime | None = None
    ):
        data = {
            **self._as_dict(),
            "assessment_start_timestamp": assessment_start or self.assessment_start_timestamp,
            "assessment_end_timestamp": assessment_end or self.assessment_end_timestamp,
        }
        return self.from_dict(data)


@dataclass
class DirectFsAccessInQuery(DirectFsAccess):

    def from_dict(self, data: dict[str, Any]) -> DirectFsAccessInQuery:
        return DirectFsAccessInQuery(**data)


@dataclass
class DirectFsAccessInPath(DirectFsAccess):
    job_id: int = -1
    job_name: str = DirectFsAccess.UNKNOWN
    task_key: str = DirectFsAccess.UNKNOWN

    def from_dict(self, data: dict[str, Any]) -> DirectFsAccessInPath:
        return DirectFsAccessInPath(**data)

    def replace_job_infos(
        self,
        job_id: int | None = None,
        job_name: str | None = None,
        task_key: str | None = None,
    ):
        data = {
            **self._as_dict(),
            "job_id": job_id or self.job_id,
            "job_name": job_name or self.job_name,
            "task_key": task_key or self.task_key,
        }
        return self.from_dict(data)


T = TypeVar("T", bound=DirectFsAccess)


class DirectFsAccessCrawler(CrawlerBase[T]):

    @classmethod
    def for_paths(cls, backend: SqlBackend, schema) -> DirectFsAccessCrawler:
        return DirectFsAccessCrawler[DirectFsAccessInPath](
            backend, schema, "direct_file_system_access_in_paths", DirectFsAccessInPath
        )

    @classmethod
    def for_queries(cls, backend: SqlBackend, schema) -> DirectFsAccessCrawler:
        return DirectFsAccessCrawler[DirectFsAccessInQuery](
            backend, schema, "direct_file_system_access_in_queries", DirectFsAccessInQuery
        )

    def __init__(self, backend: SqlBackend, schema: str, table: str, klass: type[T]):
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, table, klass)

    def append(self, dfsas: Sequence[T]):
        try:
            # TODO until we historize data, we append all DFSAs
            self._update_snapshot(dfsas, mode="append")
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def _try_fetch(self) -> Iterable[T]:
        sql = f"SELECT * FROM {escape_sql_identifier(self.full_name)}"
        for row in self._backend.fetch(sql):
            # deserialize LineageAtom from dict
            row_dict = row.as_dict()
            lineage_dicts = row_dict["source_lineage"]
            row_dict["source_lineage"] = [LineageAtom(*lineage_dict) for lineage_dict in lineage_dicts]
            yield self._klass(**row_dict)

    def _crawl(self) -> Iterable[T]:
        return []
        # TODO raise NotImplementedError() once CrawlerBase supports empty snapshots
