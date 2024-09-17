from __future__ import annotations

import dataclasses
import logging
import sys
from collections.abc import Sequence, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.framework.utils import escape_sql_identifier

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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
        sql = f"SELECT * FROM {escape_sql_identifier(self.full_name)}"
        for row in self._backend.fetch(sql):
            yield self._klass.from_dict(row.as_dict())

    def _crawl(self) -> Iterable[DirectFsAccess]:
        return []
        # TODO raise NotImplementedError() once CrawlerBase supports empty snapshots
