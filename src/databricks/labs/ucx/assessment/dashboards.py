from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyQuery

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

    name: str = "UNKNOWN"
    """The title of the dashboard that appears in list views and at the top of the dashboard page."""

    parent: str = "ORPHAN"
    """The identifier of the workspace folder containing the object."""

    query_ids: list[str] = field(default_factory=list)
    """The IDs of the queries referenced by this dashboard."""

    tags: list[str] = field(default_factory=list)
    """The tags set on this dashboard."""

    @classmethod
    def from_sdk_dashboard(cls, dashboard: SdkRedashDashboard) -> RedashDashboard:
        query_ids = []
        for widget in dashboard.widgets or []:
            if widget.visualization is None:
                continue
            if widget.visualization.query is None:
                continue
            if widget.visualization.query.id is None:
                continue
            query_ids.append(widget.visualization.query.id)
        return cls(
            id=dashboard.id or cls.id,
            name=dashboard.name or cls.name,
            parent=dashboard.parent or cls.parent,
            query_ids=query_ids,
            tags=dashboard.tags or [],
        )


class RedashDashboardCrawler(CrawlerBase[RedashDashboard]):
    """Crawler for Redash dashboards."""

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        schema: str,
        *,
        include_dashboard_ids: list[str] | None = None,
        debug_listing_upper_limit: int | None = None,
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "redash_dashboards", RedashDashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []
        self._debug_listing_upper_limit = debug_listing_upper_limit

    def _crawl(self) -> Iterable[RedashDashboard]:
        dashboards = []
        for sdk_dashboard in self._list_dashboards():
            if sdk_dashboard.id is None:
                continue
            dashboard = RedashDashboard.from_sdk_dashboard(sdk_dashboard)
            dashboards.append(dashboard)
        return dashboards

    def _list_dashboards(self) -> list[SdkRedashDashboard]:
        if self._include_dashboard_ids:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            dashboards_iterator = self._ws.dashboards.list()
        except DatabricksError as e:
            logger.warning("Cannot list Redash dashboards", exc_info=e)
            return []
        dashboards: list[SdkRedashDashboard] = []
        # Redash APIs are very slow to paginate, especially for large number of dashboards, so we limit the listing
        # to a small number of items in debug mode for the assessment workflow just to complete.
        while self._debug_listing_upper_limit is None or self._debug_listing_upper_limit > len(dashboards):
            try:
                dashboards.append(next(dashboards_iterator))
            except StopIteration:
                break
            except DatabricksError as e:
                logger.warning("Cannot list next Redash dashboards page", exc_info=e)
                break
        return dashboards

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

    def list_queries(self) -> Iterable[LegacyQuery]:
        """List queries.

        Note:
            This public method does not adhere to the common crawler layout, still, it is implemented to avoid/postpone
            another crawler for the queries by retrieving the queries every time they are requested.
        """
        try:
            yield from self._ws.queries_legacy.list()  # TODO: Update this to non-legacy query
        except DatabricksError as e:
            logger.warning("Cannot list Redash queries", exc_info=e)

    def get_query(self, query_id: str, dashboard: RedashDashboard) -> LegacyQuery | None:
        """Get a query given its id and the corresponding dashboard.

        Note:
            This public method does not adhere to the common crawler layout, still, it is implemented to avoid/postpone
            another crawler for the queries by retrieving the queries every time they are requested.
        """
        _ = dashboard
        try:
            return self._ws.queries_legacy.get(query_id)  # TODO: Update this to non-legacy query
        except DatabricksError as e:
            logger.warning(f"Cannot get Redash query: {query_id}", exc_info=e)
            return None


@dataclass
class LakeviewDashboard:
    """UCX representation of a Lakeview dashboard.

    Note: We prefer to keep this class similar to the :class:RedashDashboard.
    """

    id: str
    """The ID for this dashboard."""

    name: str = "UNKNOWN"
    """The title of the dashboard that appears in list views and at the top of the dashboard page."""

    parent: str = "ORPHAN"
    """The identifier of the workspace folder containing the object."""

    query_ids: list[str] = field(default_factory=list)
    """The IDs of the queries referenced by this dashboard."""

    @classmethod
    def from_sdk_dashboard(cls, dashboard: SdkLakeviewDashboard) -> LakeviewDashboard:
        assert dashboard.dashboard_id
        lsql_dashboard = LsqlLakeviewDashboard([], [])
        if dashboard.serialized_dashboard is not None:
            try:
                lsql_dashboard = LsqlLakeviewDashboard.from_dict(json.loads(dashboard.serialized_dashboard))
            except (KeyError, ValueError, json.JSONDecodeError) as e:
                logger.warning(f"Error when parsing Lakeview dashboard: {dashboard.dashboard_id}", exc_info=e)
        query_ids = [dataset.name for dataset in lsql_dashboard.datasets]
        return cls(
            id=dashboard.dashboard_id,
            name=dashboard.display_name or cls.name,
            parent=dashboard.parent_path or cls.parent,
            query_ids=query_ids,
        )


class LakeviewDashboardCrawler(CrawlerBase[LakeviewDashboard]):
    """Crawler for Lakeview dashboards."""

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        schema: str,
        *,
        include_dashboard_ids: list[str] | None = None,
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "lakeview_dashboards", LakeviewDashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []

    def _crawl(self) -> Iterable[LakeviewDashboard]:
        dashboards = []
        for sdk_dashboard in self._list_dashboards():
            if sdk_dashboard.dashboard_id is None:
                continue
            dashboard = LakeviewDashboard.from_sdk_dashboard(sdk_dashboard)
            dashboards.append(dashboard)
        return dashboards

    def _list_dashboards(self) -> list[SdkLakeviewDashboard]:
        if self._include_dashboard_ids:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            # If the API listing limit becomes an issue in testing, please see the `:class:RedashDashboardCrawler`
            # for an example on how to implement a (debug) rate limit
            return list(self._ws.lakeview.list())  # TODO: Add dashboard summary view?
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

    def list_queries(self) -> Iterable[str]:
        """List queries.

        Note:
            This public method does not adhere to the common crawler layout, still, it is implemented to avoid/postpone
            another crawler for the queries by retrieving the queries every time they are requested.

            Different to the Redash crawler, Lakeview queries are part of the (serialized) dashboard definition.
        """
        for dashboard in self._list_dashboards():
            if dashboard.serialized_dashboard is None:
                continue
            try:
                lsql_dashboard = LsqlLakeviewDashboard.from_dict(json.loads(dashboard.serialized_dashboard))
            except (KeyError, ValueError, json.JSONDecodeError) as e:
                logger.warning(f"Error when parsing Lakeview dashboard: {dashboard.dashboard_id}", exc_info=e)
                continue
            for dataset in lsql_dashboard.datasets:
               yield dataset.query
