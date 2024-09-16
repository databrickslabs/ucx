from __future__ import annotations

import dataclasses
import logging
from collections.abc import Sequence, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar, Self

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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        source_lineage = data.get("source_lineage", None)
        if isinstance(source_lineage, list) and len(source_lineage) > 0 and isinstance(source_lineage[0], dict):
            lineage_atoms = [LineageAtom(*lineage) for lineage in source_lineage]
            data["source_lineage"] = lineage_atoms
        return cls(**data)

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
        return dataclasses.replace(
            self,
            source_id=source_id or self.source_id,
            source_timestamp=source_timestamp or self.source_timestamp,
            source_lineage=source_lineage or self.source_lineage,
        )

    def replace_assessment_infos(
        self, assessment_start: datetime | None = None, assessment_end: datetime | None = None
    ):
        return dataclasses.replace(
            self,
            assessment_start_timestamp=assessment_start or self.assessment_start_timestamp,
            assessment_end_timestamp=assessment_end or self.assessment_end_timestamp,
        )


@dataclass
class DirectFsAccessInQuery(DirectFsAccess):

    pass


@dataclass
class DirectFsAccessInPath(DirectFsAccess):
    job_id: int = -1
    job_name: str = DirectFsAccess.UNKNOWN
    task_key: str = DirectFsAccess.UNKNOWN

    def replace_job_infos(
        self,
        job_id: int | None = None,
        job_name: str | None = None,
        task_key: str | None = None,
    ):
        return dataclasses.replace(
            self, job_id=job_id or self.job_id, job_name=job_name or self.job_name, task_key=task_key or self.task_key
        )


T = TypeVar("T", bound=DirectFsAccess)


class DirectFsAccessCrawler(CrawlerBase[T]):

    @classmethod
    def for_paths(cls, backend: SqlBackend, schema) -> DirectFsAccessCrawler:
        return DirectFsAccessCrawler[DirectFsAccessInPath](
            backend,
            schema,
            "directfs_in_paths",
            DirectFsAccessInPath,
        )

    @classmethod
    def for_queries(cls, backend: SqlBackend, schema) -> DirectFsAccessCrawler:
        return DirectFsAccessCrawler[DirectFsAccessInQuery](
            backend,
            schema,
            "directfs_in_queries",
            DirectFsAccessInQuery,
        )

    def __init__(self, backend: SqlBackend, schema: str, table: str, klass: type[T]):
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, table, klass)

    def dump_all(self, dfsas: Sequence[T]):
        """This crawler doesn't follow the pull model because the fetcher fetches data for 2 crawlers, not just one
        It's not **bad** because all records are pushed at once.
        Providing a multi-entity crawler is out-of-scope of this PR
        """
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
