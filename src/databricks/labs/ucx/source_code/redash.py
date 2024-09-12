import logging
from collections.abc import Iterator
from dataclasses import replace

from databricks.labs.blueprint.installation import Installation

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Dashboard, LegacyQuery, UpdateQueryRequestQuery
from databricks.sdk.errors.platform import DatabricksError

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.from_table import FromTableSqlLinter

logger = logging.getLogger(__name__)


class Redash:
    MIGRATED_TAG = "Migrated by UCX"

    def __init__(self, index: TableMigrationIndex, ws: WorkspaceClient, installation: Installation):
        self._index = index
        self._ws = ws
        self._installation = installation

    def migrate_dashboards(self, dashboard_id: str | None = None):
        for dashboard in self._list_dashboards(dashboard_id):
            assert dashboard.id is not None
            if dashboard.tags is not None and self.MIGRATED_TAG in dashboard.tags:
                logger.debug(f"Dashboard {dashboard.name} already migrated by UCX")
                continue
            for query in self.get_queries_from_dashboard(dashboard):
                self._fix_query(query)
            self._ws.dashboards.update(dashboard.id, tags=self._get_migrated_tags(dashboard.tags))

    def revert_dashboards(self, dashboard_id: str | None = None):
        for dashboard in self._list_dashboards(dashboard_id):
            assert dashboard.id is not None
            if dashboard.tags is None or self.MIGRATED_TAG not in dashboard.tags:
                logger.debug(f"Dashboard {dashboard.name} was not migrated by UCX")
                continue
            for query in self.get_queries_from_dashboard(dashboard):
                self._revert_query(query)
            self._ws.dashboards.update(dashboard.id, tags=self._get_original_tags(dashboard.tags))

    def _list_dashboards(self, dashboard_id: str | None) -> list[Dashboard]:
        try:
            if dashboard_id is None:
                return list(self._ws.dashboards.list())
            return [self._ws.dashboards.get(dashboard_id)]
        except DatabricksError as e:
            logger.error(f"Cannot list dashboards: {e}")
            return []

    def _fix_query(self, query: LegacyQuery):
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
    def _get_session_state(query: LegacyQuery) -> CurrentSessionState:
        session_state = CurrentSessionState()
        if query.options is None:
            return session_state
        if query.options.catalog:
            session_state = replace(session_state, catalog=query.options.catalog)
        if query.options.schema:
            session_state = replace(session_state, schema=query.options.schema)
        return session_state

    def _revert_query(self, query: LegacyQuery):
        assert query.id is not None
        assert query.query is not None
        if query.tags is None:
            return
        # find the backup query
        is_migrated = False
        for tag in query.tags:
            if tag == self.MIGRATED_TAG:
                is_migrated = True

        if not is_migrated:
            logger.debug(f"Query {query.name} was not migrated by UCX")
            return

        backup_query = self._installation.load(LegacyQuery, filename=f'backup/queries/{query.id}.json')
        update_query = UpdateQueryRequestQuery(
            query_text=backup_query.query, tags=self._get_original_tags(backup_query.tags)
        )
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

    @staticmethod
    def get_queries_from_dashboard(dashboard: Dashboard) -> Iterator[LegacyQuery]:
        if dashboard.widgets is None:
            return
        for widget in dashboard.widgets:
            if widget is None:
                continue
            if widget.visualization is None:
                continue
            if widget.visualization.query is None:
                continue
            yield widget.visualization.query
