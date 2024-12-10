from __future__ import annotations

import itertools
import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard, Dataset
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyQuery

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier


logger = logging.getLogger(__name__)


@dataclass
class Query:
    """UCX representation of a Query.

    Note:
        This class is not persisted into an inventory table. If you decide to persist this class, consider (future)
        differences between Redash and Lakeview queries
    """

    id: str
    """The ID for this query."""

    name: str = "UNKNOWN"
    """The title of this query that appears in list views, widget headings, and on the query page."""

    parent: str = "ORPHAN"
    """The identifier of the workspace folder containing the object."""

    query: str = ""
    """The text of the query to be run."""

    catalog: str = ""
    """The name of the catalog to execute this query in."""

    schema: str = ""
    """The name of the schema to execute this query in."""

    tags: list[str] = field(default_factory=list)
    """The tags set on this dashboard."""

    @classmethod
    def from_legacy_query(cls, query: LegacyQuery) -> Query:
        """Create query from a :class:LegacyQuery"""
        assert query.id
        catalog = schema = None
        if query.options:
            catalog = query.options.catalog
            schema = query.options.schema
        return cls(
            id=query.id,
            name=query.name or cls.name,
            parent=query.parent or cls.parent,
            query=query.query or cls.query,
            catalog=catalog or cls.catalog,
            schema=schema or cls.schema,
            tags=query.tags or [],
        )

    @classmethod
    def from_lakeview_dataset(cls, dataset: Dataset, *, parent: str | None = None) -> Query:
        """Create query from a :class:Dataset"""
        return cls(
            id=dataset.name,
            name=dataset.display_name or cls.name,
            parent=parent or cls.parent,
            query=dataset.query,
        )


@dataclass
class Dashboard:
    """UCX representation of a dashboard."""

    id: str
    """The ID for this dashboard."""

    name: str | None = None
    """The title of the dashboard that appears in list views and at the top of the dashboard page."""

    parent: str | None = None
    """The identifier of the workspace folder containing the object."""

    query_ids: list[str] = field(default_factory=list)
    """The IDs of the queries referenced by this dashboard."""

    tags: list[str] = field(default_factory=list)
    """The tags set on this dashboard."""

    @classmethod
    def from_sdk_redash_dashboard(cls, dashboard: SdkRedashDashboard) -> Dashboard:
        assert dashboard.id
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
            id=dashboard.id,
            name=dashboard.name or cls.name,
            parent=dashboard.parent or cls.parent,
            query_ids=query_ids,
            tags=dashboard.tags or [],
        )

    @classmethod
    def from_sdk_lakeview_dashboard(cls, dashboard: SdkLakeviewDashboard) -> Dashboard:
        assert dashboard.dashboard_id
        lsql_dashboard = _convert_sdk_to_lsql_lakeview_dashboard(dashboard)
        query_ids = [dataset.name for dataset in lsql_dashboard.datasets]
        return cls(
            id=dashboard.dashboard_id,
            name=dashboard.display_name or cls.name,
            parent=dashboard.parent_path or cls.parent,
            query_ids=query_ids,
        )


class RedashDashboardCrawler(CrawlerBase[Dashboard]):
    """Crawler for Redash dashboards."""

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        schema: str,
        *,
        include_dashboard_ids: list[str] | None = None,
        include_query_ids: list[str] | None = None,
        debug_listing_upper_limit: int | None = None,
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "redash_dashboards", Dashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []
        self._include_query_ids = include_query_ids or []
        self._debug_listing_upper_limit = debug_listing_upper_limit

    def _crawl(self) -> Iterable[Dashboard]:
        dashboards = []
        for sdk_dashboard in self._list_dashboards():
            if sdk_dashboard.id is None:
                continue
            dashboard = Dashboard.from_sdk_redash_dashboard(sdk_dashboard)
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

    def _try_fetch(self) -> Iterable[Dashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Dashboard(*row)

    def list_queries(self, dashboard: Dashboard | None = None) -> Iterator[Query]:
        """List queries.

        Args:
            dashboard (DashboardType | None) : List queries for dashboard. If None, list all queries.
                Defaults to None.

        Note:
            This public method does not adhere to the common crawler layout, still, it is implemented to avoid/postpone
            another crawler for the queries by retrieving the queries every time they are requested.
        """
        for query in self._list_legacy_queries(dashboard):
            yield Query.from_legacy_query(query)

    def _list_legacy_queries(self, dashboard: Dashboard | None = None) -> Iterator[LegacyQuery]:
        """List legacy queries.

        Args:
            dashboard (DashboardType | None) : List queries for dashboard. If None, list all queries.
                Defaults to None.

        Note:
            This public method does not adhere to the common crawler layout, still, it is implemented to avoid/postpone
            another crawler for the queries by retrieving the queries every time they are requested.
        """
        if dashboard:
            queries_iterator = self._list_legacy_queries_from_dashboard(dashboard)
        else:
            queries_iterator = self._list_all_legacy_queries()
        # Redash APIs are very slow to paginate, especially for large number of dashboards, so we limit the listing
        # to a small number of items in debug mode for the assessment workflow just to complete.
        counter = itertools.count()
        while self._debug_listing_upper_limit is None or self._debug_listing_upper_limit > next(counter):
            try:
                yield next(queries_iterator)
            except StopIteration:
                break

    def _list_all_legacy_queries(self) -> Iterator[LegacyQuery]:
        """List all queries."""
        if self._include_query_ids:
            yield from self._get_legacy_queries(*self._include_query_ids)
        else:
            try:
                yield from self._ws.queries_legacy.list()  # TODO: Update this to non-legacy query
            except DatabricksError as e:
                logger.warning("Cannot list Redash queries", exc_info=e)

    def _list_legacy_queries_from_dashboard(self, dashboard: Dashboard) -> Iterator[LegacyQuery]:
        """List queries from dashboard."""
        if self._include_query_ids:
            query_ids = set(dashboard.query_ids) & set(self._include_query_ids)
        else:
            query_ids = set(dashboard.query_ids)
        yield from self._get_legacy_queries(*query_ids)

    def _get_legacy_queries(self, *query_ids: str) -> Iterator[LegacyQuery]:
        """Get a legacy queries."""
        for query_id in query_ids:
            query = self._get_legacy_query(query_id)
            if query:
                yield query

    def _get_legacy_query(self, query_id: str) -> LegacyQuery | None:
        """Get a legacy query."""
        try:
            return self._ws.queries_legacy.get(query_id)  # TODO: Update this to non-legacy query
        except DatabricksError as e:
            logger.warning(f"Cannot get Redash query: {query_id}", exc_info=e)
            return None


def _convert_sdk_to_lsql_lakeview_dashboard(dashboard: SdkLakeviewDashboard) -> LsqlLakeviewDashboard:
    """Parse a lsql Lakeview dashboard from an SDK Lakeview dashboard.

    Returns :
        LsqlLakeviewDashboard : The parsed dashboard. If the parsing fails, it is an empty dashboard, i.e. a
            dashboard without datasets and pages.
    """
    lsql_dashboard = LsqlLakeviewDashboard([], [])
    if dashboard.serialized_dashboard is not None:
        try:
            lsql_dashboard = LsqlLakeviewDashboard.from_dict(json.loads(dashboard.serialized_dashboard))
        except (KeyError, ValueError) as e:
            logger.warning(f"Error when parsing Lakeview dashboard: {dashboard.dashboard_id}", exc_info=e)
    return lsql_dashboard


class LakeviewDashboardCrawler(CrawlerBase[Dashboard]):
    """Crawler for Lakeview dashboards."""

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        schema: str,
        *,
        include_dashboard_ids: list[str] | None = None,
        include_query_ids: list[str] | None = None,
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "lakeview_dashboards", Dashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids or []
        self._include_query_ids = include_query_ids or []

    def _crawl(self) -> Iterable[Dashboard]:
        dashboards = []
        for sdk_dashboard in self._list_dashboards():
            if sdk_dashboard.dashboard_id is None:
                continue
            dashboard = Dashboard.from_sdk_lakeview_dashboard(sdk_dashboard)
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

    def _try_fetch(self) -> Iterable[Dashboard]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Dashboard(*row)

    def list_queries(self, dashboard: Dashboard | None = None) -> Iterator[Query]:
        """List queries.

        Args:
            dashboard (DashboardType | None) : List queries for dashboard. If None, list all queries.
                Defaults to None.

        Note:
            This public method does not adhere to the common crawler layout, still, it is implemented to avoid/postpone
            another crawler for the queries by retrieving the queries every time they are requested.

            Different to the Redash crawler, Lakeview queries are part of the (serialized) dashboard definition.
        """
        sdk_dashboards = []
        if dashboard:
            sdk_dashboard = self._get_dashboard(dashboard_id=dashboard.id)
            if sdk_dashboard:
                sdk_dashboards.append(sdk_dashboard)
        else:
            sdk_dashboards = self._list_dashboards()
        for sdk_dashboard in sdk_dashboards:
            lsql_dashboard = _convert_sdk_to_lsql_lakeview_dashboard(sdk_dashboard)
            for dataset in lsql_dashboard.datasets:
                if self._include_query_ids and dataset.name not in self._include_query_ids:
                    continue
                yield Query.from_lakeview_dataset(dataset, parent=sdk_dashboard.dashboard_id)
