import logging
from collections.abc import Iterator
from dataclasses import replace

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query, Dashboard
from databricks.sdk.errors.platform import DatabricksError, NotFound, ResourceDoesNotExist

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.queries import FromTable

logger = logging.getLogger(__name__)


class Redash:
    MIGRATED_TAG = "migrated by UCX"
    BACKUP_TAG = "backup by UCX"

    def __init__(self, index: MigrationIndex, ws: WorkspaceClient, backup_path: str):
        self._index = index
        self._ws = ws
        self._backup_path = backup_path + "/backup_queries"

    def fix_dashboard(self, dashboard_id: str | None = None):
        for dashboard in self._list_dashboards(dashboard_id):
            assert dashboard.id is not None
            if dashboard.tags is not None and self.MIGRATED_TAG in dashboard.tags:
                logger.debug(f"Dashboard {dashboard.name} already migrated by UCX")
                return
            for query in self.get_queries_from_dashboard(dashboard):
                self._fix_query(query)
            self._ws.dashboards.update(dashboard.id, tags=self._get_migrated_tags(dashboard.tags))

    def revert_dashboard(self, dashboard_id: str | None = None):
        for dashboard in self._list_dashboards(dashboard_id):
            assert dashboard.id is not None
            if dashboard.tags is None or self.MIGRATED_TAG not in dashboard.tags:
                logger.debug(f"Dashboard {dashboard.name} was not migrated by UCX")
                return
            for query in self.get_queries_from_dashboard(dashboard):
                self._revert_query(query)
            self._ws.dashboards.update(dashboard.id, tags=self._get_original_tags(dashboard.tags))

    def _list_dashboards(self, dashboard_id: str | None) -> list[Dashboard]:
        try:
            if dashboard_id is None:
                return list(self._ws.dashboards.list())
            return [self._ws.dashboards.get(dashboard_id)]
        except DatabricksError as e:
            logger.debug(f"Cannot list dashboards: {e}")
            return []

    def delete_backup_dbsql_queries(self):
        for query in self._ws.queries.list():
            if query.tags is None or self.BACKUP_TAG not in query.tags:
                continue
            try:
                self._ws.queries.delete(query.id)
            except NotFound:
                continue

    def _fix_query(self, query: Query):
        assert query.id is not None
        assert query.query is not None
        if query.tags is not None and self.MIGRATED_TAG in query.tags:
            # already migrated
            return
        # create the backup folder for queries if it does not exist
        try:
            self._ws.workspace.get_status(self._backup_path)
        except ResourceDoesNotExist:
            self._ws.workspace.mkdirs(self._backup_path)
        backup_query = self._ws.queries.create(
            data_source_id=query.data_source_id,
            description=query.description,
            name=str(query.name) + "_original",
            options=query.options.as_dict() if query.options is not None else None,
            parent=self._backup_path,
            query=query.query,
            run_as_role=query.run_as_role,
            tags=[self.BACKUP_TAG],
        )
        from_table = FromTable(self._index, self._get_session_state(query))
        new_query = from_table.apply(query.query)
        try:
            self._ws.queries.update(
                query.id,
                query=new_query,
                tags=self._get_migrated_tags(query.tags, [f'backup:{backup_query.id}']),
            )
        except DatabricksError:
            logger.error(f"Cannot upgrade {query.name}")
            return

    @staticmethod
    def _get_session_state(query: Query) -> CurrentSessionState:
        session_state = CurrentSessionState()
        if query.options is None:
            return session_state
        if query.options.catalog:
            session_state = replace(session_state, catalog=query.options.catalog)
        if query.options.schema:
            session_state = replace(session_state, schema=query.options.schema)
        return session_state

    def _revert_query(self, query: Query):
        assert query.id is not None
        assert query.query is not None
        if query.tags is None:
            return
        # find the backup query
        is_migrated = False
        backup_id = None
        for tag in query.tags:
            if tag == self.MIGRATED_TAG:
                is_migrated = True
                continue
            if tag.startswith("backup:"):
                backup_id = tag.split(":")[1]
                continue

        if not is_migrated:
            logger.debug(f"Query {query.name} was not migrated by UCX")
            return

        if backup_id is None:
            logger.debug(f"Cannot find backup query for query {query.name}")
            return
        try:
            original_query = self._ws.queries.get(backup_id)
            self._ws.queries.update(query.id, query=original_query.query, tags=self._get_original_tags(query.tags))
            self._ws.queries.delete(backup_id)
        except DatabricksError:
            logger.error(f"Cannot restore {query.name} from backup query {backup_id}")
            return

    def _get_migrated_tags(self, tags: list[str] | None, new_tags: list[str] | None = None) -> list[str]:
        out = [self.MIGRATED_TAG]
        if tags:
            out.extend(tags)
        if new_tags:
            out.extend(new_tags)
        return out

    def _get_original_tags(self, tags: list[str] | None) -> list[str] | None:
        if tags is None:
            return None
        return [
            tag for tag in tags if not tag.startswith("backup:") and tag not in (self.MIGRATED_TAG, self.BACKUP_TAG)
        ]

    @staticmethod
    def get_queries_from_dashboard(dashboard: Dashboard) -> Iterator[Query]:
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
