from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier


logger = logging.getLogger(__name__)


@dataclass
class RedashDashboard:
    """UCX representation of a Redash dashboard.

    Note: We prefer to keep this class similar to the :class:LakeviewDashboard.
    """

    id: str
    """The ID for this dashboard."""

    @classmethod
    def from_sdk_dashboard(cls, dashboard: SdkRedashDashboard) -> RedashDashboard:
        assert dashboard.id
        return cls(id=dashboard.id)


class RedashDashBoardCrawler(CrawlerBase[RedashDashboard]):
    """Crawler for Redash dashboards."""

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, schema: str, include_dashboard_ids: list[str] | None = None):
        super().__init__(sql_backend, "hive_metastore", schema, "redash_dashboards", RedashDashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []

    def _crawl(self) -> Iterable[RedashDashboard]:
        dashboards = [RedashDashboard.from_sdk_dashboard(dashboard) for dashboard in self._list_dashboards()]
        return dashboards

    def _list_dashboards(self) -> list[SdkRedashDashboard]:
        if self._include_dashboard_ids:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            return list(self._ws.dashboards.list())
        except DatabricksError as e:
            logger.warning("Cannot list Redash dashboards", exc_info=e)
            return []

    def _get_dashboards(self, *dashboard_ids: str) -> list[SdkRedashDashboard]:
        dashboards = []
        for dashboard_id in dashboard_ids:
            dashboard = self._get_dashboard(dashboard_id)
            if dashboard:
                dashboards.append(dashboard)
        return dashboards

    def _get_dashboard(self, dashboard_id: str) -> SdkRedashDashboard | None:
        try:
            return self._ws.dashboards.get(dashboard_id)
        except DatabricksError as e:
            logger.warning(f"Cannot get Redash dashboard: {dashboard_id}", exc_info=e)
            return None

    def _try_fetch(self) -> Iterable[RedashDashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield RedashDashboard(*row)


@dataclass
class LakeviewDashboard:
    """UCX representation of a Lakeview dashboard.

    Note: We prefer to keep this class similar to the :class:RedashDashboard.
    """

    id: str
    """The ID for this dashboard."""

    @classmethod
    def from_sdk_dashboard(cls, dashboard: SdkLakeviewDashboard) -> LakeviewDashboard:
        assert dashboard.dashboard_id
        return cls(id=dashboard.dashboard_id)


class LakeviewDashboardCrawler(CrawlerBase[LakeviewDashboard]):
    """Crawler for Lakeview dashboards."""

    def __init__(self, ws: WorkspaceClient, sql_backend: SqlBackend, schema: str, include_dashboard_ids: list[str] | None = None):
        super().__init__(sql_backend, "hive_metastore", schema, "lakeview_dashboards", LakeviewDashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []

    def _crawl(self) -> Iterable[LakeviewDashboard]:
        dashboards = [LakeviewDashboard.from_sdk_dashboard(dashboard) for dashboard in self._list_dashboards()]
        return dashboards

    def _list_dashboards(self) -> list[SdkLakeviewDashboard]:
        if self._include_dashboard_ids:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            return list(self._ws.lakeview.list())
        except DatabricksError as e:
            logger.warning("Cannot list Lakeview dashboards", exc_info=e)
            return []

    def _get_dashboards(self, *dashboard_ids: str) -> list[SdkLakeviewDashboard]:
        dashboards = []
        for dashboard_id in dashboard_ids:
            dashboard = self._get_dashboard(dashboard_id)
            if dashboard:
                dashboards.append(dashboard)
        return dashboards

    def _get_dashboard(self, dashboard_id: str) -> SdkLakeviewDashboard | None:
        try:
            return self._ws.lakeview.get(dashboard_id)
        except DatabricksError as e:
            logger.warning(f"Cannot get Lakeview dashboard: {dashboard_id}", exc_info=e)
            return None

    def _try_fetch(self) -> Iterable[LakeviewDashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield LakeviewDashboard(*row)
