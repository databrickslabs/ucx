from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard
from databricks.sdk.service.sql import Dashboard as SqlDashboard

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier


logger = logging.getLogger(__name__)


@dataclass
class Dashboard:
    """UCX representation of a dashboard"""

    id: str
    """The ID for this dashboard."""

    @classmethod
    def from_sql_dashboard(cls, dashboard: SqlDashboard) -> Dashboard:
        assert dashboard.id
        return cls(id=dashboard.id)

    @classmethod
    def from_sdk_dashboard(cls, dashboard: SDKDashboard) -> Dashboard:
        assert dashboard.dashboard_id
        return cls(id=dashboard.dashboard_id)


class RedashDashBoardCrawler(CrawlerBase[Dashboard]):
    """Crawler for Redash dashboards."""

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, schema: str, include_dashboard_ids: list[str] | None = None):
        super().__init__(sql_backend, "hive_metastore", schema, "redash_dashboards", Dashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []

    def _crawl(self) -> Iterable[Dashboard]:
        dashboards = [Dashboard.from_sql_dashboard(dashboard) for dashboard in self._list_dashboards()]
        return dashboards

    def _list_dashboards(self) -> list[SqlDashboard]:
        if self._include_dashboard_ids:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            return list(self._ws.dashboards.list())
        except DatabricksError as e:
            logger.warning("Cannot list dashboards", exc_info=e)
            return []

    def _get_dashboards(self, *dashboard_ids: str) -> list[SqlDashboard]:
        dashboards = []
        for dashboard_id in dashboard_ids:
            dashboard = self._get_dashboard(dashboard_id)
            if dashboard:
                dashboards.append(dashboard)
        return dashboards

    def _get_dashboard(self, dashboard_id: str) -> Dashboard | None:
        try:
            return self._ws.dashboards.get(dashboard_id)
        except DatabricksError as e:
            logger.warning(f"Cannot get dashboard: {dashboard_id}", exc_info=e)
            return None

    def _try_fetch(self) -> Iterable[Dashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Dashboard(*row)


class LakeviewDashboardCrawler(CrawlerBase[Dashboard]):
    """Crawler for Lakeview dashboards."""

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, schema: str, include_dashboard_ids: list[str] | None = None):
        super().__init__(sql_backend, "hive_metastore", schema, "lakeview_dashboards", Dashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []

    def _crawl(self) -> Iterable[Dashboard]:
        dashboards = [Dashboard.from_sdk_dashboard(dashboard) for dashboard in self._list_dashboards()]
        return dashboards

    def _list_dashboards(self) -> list[SDKDashboard]:
        if self._include_dashboard_ids:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            return list(self._ws.lakeview.list())
        except DatabricksError as e:
            logger.warning("Cannot list dashboards", exc_info=e)
            return []

    def _get_dashboards(self, *dashboard_ids: str) -> list[SDKDashboard]:
        dashboards = []
        for dashboard_id in dashboard_ids:
            dashboard = self._get_dashboard(dashboard_id)
            if dashboard:
                dashboards.append(dashboard)
        return dashboards

    def _get_dashboard(self, dashboard_id: str) -> SDKDashboard | None:
        try:
            return self._ws.lakeview.get(dashboard_id)
        except DatabricksError as e:
            logger.warning(f"Cannot get dashboard: {dashboard_id}", exc_info=e)
            return None

    def _try_fetch(self) -> Iterable[Dashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Dashboard(*row)
