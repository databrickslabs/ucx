from __future__ import annotations

import itertools
import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import ClassVar

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard, Dataset
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyQuery

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.owners import AdministratorLocator, Ownership, WorkspacePathOwnership
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

    name: str | None = None
    """The title of this query that appears in list views, widget headings, and on the query page."""

    parent: str | None = None
    """The identifier of the workspace folder containing the object."""

    query: str | None = None
    """The text of the query to be run."""

    catalog: str | None = None
    """The name of the catalog to execute this query in."""

    schema: str | None = None
    """The name of the schema to execute this query in."""

    tags: list[str] = field(default_factory=list)
    """The tags set on this dashboard."""

    @classmethod
    def from_legacy_query(cls, query: LegacyQuery) -> Query:
        """Create query from a :class:LegacyQuery"""
        if not query.id:
            raise ValueError(f"Query id is required: {query}")
        kwargs: dict[str, str | list[str]] = {"id": query.id}
        if query.name:
            kwargs["name"] = query.name
        if query.parent:
            kwargs["parent"] = query.parent
        if query.query:
            kwargs["query"] = query.query
        if query.options and query.options.catalog:
            kwargs["catalog"] = query.options.catalog
        if query.options and query.options.schema:
            kwargs["schema"] = query.options.schema
        if query.tags:
            kwargs["tags"] = query.tags
        return cls(**kwargs)  # type: ignore

    @classmethod
    def from_lakeview_dataset(cls, dataset: Dataset, *, parent: str | None = None) -> Query:
        """Create query from a :class:Dataset"""
        if not dataset.name:
            raise ValueError(f"Dataset name is required: {dataset}")
        kwargs = {"id": dataset.name}
        if dataset.display_name:
            kwargs["name"] = dataset.display_name
        if parent:
            kwargs["parent"] = parent
        if dataset.query:
            kwargs["query"] = dataset.query
        return cls(**kwargs)  # type: ignore


@dataclass
class Dashboard:
    """UCX representation of a dashboard."""

    __id_attributes__: ClassVar[tuple[str, ...]] = ("id",)

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

    creator_id: str | None = None
    """The ID of the user who owns the dashboard."""

    @classmethod
    def from_sdk_redash_dashboard(cls, dashboard: SdkRedashDashboard) -> Dashboard:
        assert dashboard.id
        kwargs: dict[str, str | list[str] | None] = {"id": dashboard.id}
        if dashboard.name:
            kwargs["name"] = dashboard.name
        if dashboard.parent:
            kwargs["parent"] = dashboard.parent
        if dashboard.tags:
            kwargs["tags"] = dashboard.tags
        if dashboard.user_id:
            kwargs["creator_id"] = str(dashboard.user_id)
        query_ids = []
        for widget in dashboard.widgets or []:
            if widget.visualization is None:
                continue
            if widget.visualization.query is None:
                continue
            if widget.visualization.query.id is None:
                continue
            query_ids.append(widget.visualization.query.id)
        if query_ids:
            kwargs["query_ids"] = query_ids
        return cls(**kwargs)  # type: ignore

    @classmethod
    def from_sdk_lakeview_dashboard(cls, dashboard: SdkLakeviewDashboard) -> Dashboard:
        assert dashboard.dashboard_id
        kwargs: dict[str, str | list[str] | None] = {"id": dashboard.dashboard_id}
        if dashboard.display_name:
            kwargs["name"] = dashboard.display_name
        if dashboard.parent_path:
            kwargs["parent"] = dashboard.parent_path
        lsql_dashboard = _convert_sdk_to_lsql_lakeview_dashboard(dashboard)
        query_ids = [dataset.name for dataset in lsql_dashboard.datasets]
        if query_ids:
            kwargs["query_ids"] = query_ids
        return cls(**kwargs)  # type: ignore


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
        self._include_dashboard_ids = include_dashboard_ids
        self._include_query_ids = include_query_ids
        self._debug_listing_upper_limit = debug_listing_upper_limit

    def _crawl(self) -> Iterable[Dashboard]:
        dashboards = []
        for sdk_dashboard in self._list_dashboards():
            dashboard = Dashboard.from_sdk_redash_dashboard(sdk_dashboard)
            dashboards.append(dashboard)
        return dashboards

    def _list_dashboards(self) -> list[SdkRedashDashboard]:
        if self._include_dashboard_ids is not None:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            dashboards_iterator = self._ws.dashboards.list()
        except DatabricksError as e:
            logger.error("Cannot list Redash dashboards", exc_info=e)
            return []
        dashboards: list[SdkRedashDashboard] = []
        # Redash APIs are very slow to paginate, especially for large number of dashboards, so we limit the listing
        # to a small number of items in debug mode for the assessment workflow just to complete.
        while self._debug_listing_upper_limit is None or self._debug_listing_upper_limit > len(dashboards):
            try:
                dashboard = next(dashboards_iterator)
                if dashboard.id is None:
                    continue
                #     Dashboard details are not available in the listing, so we need to fetch them
                dashboard_details = self._get_dashboard(dashboard.id)
                if dashboard_details:
                    dashboards.append(dashboard_details)
            except StopIteration:
                break
            except DatabricksError as e:
                logger.error("Cannot list next Redash dashboards page", exc_info=e)
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
        if self._include_query_ids is not None:
            yield from self._get_legacy_queries(*self._include_query_ids)
        else:
            try:
                yield from self._ws.queries_legacy.list()
            except DatabricksError as e:
                logger.error("Cannot list Redash queries", exc_info=e)

    def _list_legacy_queries_from_dashboard(self, dashboard: Dashboard) -> Iterator[LegacyQuery]:
        """List queries from dashboard."""
        if self._include_query_ids is not None:
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
            return self._ws.queries_legacy.get(query_id)
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
        exclude_dashboard_ids: list[str] | None = None,
        include_query_ids: list[str] | None = None,
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "lakeview_dashboards", Dashboard)
        self._ws = ws
        self._include_dashboard_ids = include_dashboard_ids
        self._exclude_dashboard_ids = exclude_dashboard_ids
        self._include_query_ids = include_query_ids

    def _crawl(self) -> Iterable[Dashboard]:
        dashboards = []
        for sdk_dashboard in self._list_dashboards():
            if sdk_dashboard.dashboard_id is None:
                continue
            if sdk_dashboard.dashboard_id in (self._exclude_dashboard_ids or []):
                continue
            dashboard = Dashboard.from_sdk_lakeview_dashboard(sdk_dashboard)
            dashboards.append(dashboard)
        return dashboards

    def _list_dashboards(self) -> list[SdkLakeviewDashboard]:
        if self._include_dashboard_ids is not None:
            return self._get_dashboards(*self._include_dashboard_ids)
        try:
            # If the API listing limit becomes an issue in testing, please see the `:class:RedashDashboardCrawler`
            # for an example on how to implement a (debug) rate limit
            return list(self._ws.lakeview.list())  # TODO: Add dashboard summary view?
        except DatabricksError as e:
            logger.error("Cannot list Lakeview dashboards", exc_info=e)
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
        if dashboard:
            sdk_dashboard = self._get_dashboard(dashboard_id=dashboard.id)
            sdk_dashboards = [sdk_dashboard] if sdk_dashboard else []
        else:
            sdk_dashboards = self._list_dashboards()
        for sdk_dashboard in sdk_dashboards:
            lsql_dashboard = _convert_sdk_to_lsql_lakeview_dashboard(sdk_dashboard)
            for dataset in lsql_dashboard.datasets:
                if self._include_query_ids is not None and dataset.name not in self._include_query_ids:
                    continue
                yield Query.from_lakeview_dataset(dataset, parent=sdk_dashboard.dashboard_id)


class DashboardOwnership(Ownership[Dashboard]):
    """Determine ownership of dashboard in the inventory.

    This is the dashboard creator (if known) otherwise the parent (path) owner (if known).
    """

    def __init__(
        self,
        administrator_locator: AdministratorLocator,
        ws: WorkspaceClient,
        workspace_path_ownership: WorkspacePathOwnership,
    ) -> None:
        super().__init__(administrator_locator)
        self._ws = ws
        self._workspace_path_ownership = workspace_path_ownership

    def _maybe_direct_owner(self, record: Dashboard) -> str | None:
        if record.creator_id:
            creator_name = self._get_user_name(record.creator_id)
            if creator_name:
                return creator_name
        if record.parent:
            return self._workspace_path_ownership.owner_of_path(record.parent)
        return None

    def _get_user_name(self, user_id: str) -> str | None:
        try:
            user = self._ws.users.get(user_id)
            return user.user_name
        except DatabricksError as e:
            logger.warning(f"Could not retrieve user: {user_id}", exc_info=e)
            return None
