import logging
from dataclasses import replace

from databricks.labs.blueprint.installation import Installation, SerdeError

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import LegacyQuery, UpdateQueryRequestQuery
from databricks.sdk.errors.platform import DatabricksError

from databricks.labs.ucx.assessment.dashboards import Dashboard, RedashDashboardCrawler, Query
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.from_table import FromTableSqlLinter

logger = logging.getLogger(__name__)


class Redash:
    MIGRATED_TAG = "Migrated by UCX"

    def __init__(
        self,
        index: TableMigrationIndex,
        ws: WorkspaceClient,
        installation: Installation,
        dashboard_crawler: RedashDashboardCrawler,
    ):
        self._index = index
        self._ws = ws
        self._installation = installation
        self._crawler = dashboard_crawler

    def migrate_dashboards(self, *dashboard_ids: str) -> None:
        for dashboard in self._list_dashboards(*dashboard_ids):
            if self.MIGRATED_TAG in dashboard.tags:
                logger.debug(f"Dashboard {dashboard.name} already migrated by UCX")
                continue
            for query in self._crawler.list_queries(dashboard):
                self._fix_query(query)
            self._ws.dashboards.update(dashboard.id, tags=self._get_migrated_tags(dashboard.tags))

    def revert_dashboards(self, *dashboard_ids: str) -> None:
        for dashboard in self._list_dashboards(*dashboard_ids):  # Refresh for up-to-date tags
            if self.MIGRATED_TAG not in dashboard.tags:
                logger.debug(f"Dashboard {dashboard.name} was not migrated by UCX")
                continue
            for query in self._crawler.list_queries(dashboard):
                self._revert_query(query)
            self._ws.dashboards.update(dashboard.id, tags=self._get_original_tags(dashboard.tags))

    def _list_dashboards(self, *dashboard_ids: str) -> list[Dashboard]:
        """List the Redash dashboards."""
        # Cached property is not used as this class in used from the CLI, thus called once per Python process
        dashboards = [d for d in self._crawler.snapshot() if not dashboard_ids or d.id in dashboard_ids]
        return dashboards

    def _fix_query(self, query: Query) -> None:
        assert query.id is not None
        assert query.query is not None
        # query already migrated
        if query.tags is not None and self.MIGRATED_TAG in query.tags:
            return
        # backup the query
        self._installation.save(query, filename=f'backup/queries/{query.id}.json')
        from_table = FromTableSqlLinter(self._index, self._get_session_state(query))
        new_query = UpdateQueryRequestQuery(
            query_text=from_table.apply(query.query),
            tags=self._get_migrated_tags(query.tags),
        )
        try:
            self._ws.queries.update(
                query.id,
                update_mask="query_text,tags",
                query=new_query,
            )
        except DatabricksError:
            logger.warning(f"Cannot upgrade {query.name}")
            return

    @staticmethod
    def _get_session_state(query: Query) -> CurrentSessionState:
        session_state = CurrentSessionState()
        if query.catalog:
            session_state = replace(session_state, catalog=query.catalog)
        if query.schema:
            session_state = replace(session_state, schema=query.schema)
        return session_state

    def _revert_query(self, query: Query) -> None:
        assert query.id is not None
        assert query.query is not None
        if self.MIGRATED_TAG not in (query.tags or []):
            logger.debug(f"Query {query.name} was not migrated by UCX")
            return
        backup_query: Query | LegacyQuery
        try:
            backup_query = self._installation.load(Query, filename=f'backup/queries/{query.id}.json')
        except SerdeError:
            # Previous versions store queries as LegacyQuery
            backup_query = self._installation.load(LegacyQuery, filename=f'backup/queries/{query.id}.json')
        update_query = UpdateQueryRequestQuery(query_text=backup_query.query, tags=self._get_original_tags(query.tags))
        try:
            self._ws.queries.update(query.id, update_mask="query_text,tags", query=update_query)
        except DatabricksError:
            logger.warning(f"Cannot restore {query.name} from backup")
            return

    def _get_migrated_tags(self, tags: list[str] | None) -> list[str]:
        out = [self.MIGRATED_TAG]
        if tags:
            out.extend(tags)
        return out

    def _get_original_tags(self, tags: list[str] | None) -> list[str] | None:
        if tags is None:
            return None
        return [tag for tag in tags if tag != self.MIGRATED_TAG]
