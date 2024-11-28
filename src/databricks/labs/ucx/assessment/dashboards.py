import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier


logger = logging.getLogger(__name__)


@dataclass
class Dashboard:
    """UCX representation of a dashboard"""


class RedashDashBoardCrawler(CrawlerBase[Dashboard]):
    """Crawler for Redash dashboards."""

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, schema: str):
        super().__init__(sql_backend, "hive_metastore", schema, "redash_dashboards", Dashboard)
        self._ws = ws

    def _crawl(self) -> Iterable[Dashboard]:
        return []

    def _try_fetch(self) -> Iterable[Dashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Dashboard(*row)


class LakeviewDashboardCrawler(CrawlerBase[Dashboard]):
    """Crawler for Lakeview dashboards."""

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, schema: str):
        super().__init__(sql_backend, "hive_metastore", schema, "lakeview_dashboards", Dashboard)
        self._ws = ws

    def _crawl(self) -> Iterable[Dashboard]:
        return []

    def _try_fetch(self) -> Iterable[Dashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Dashboard(*row)
