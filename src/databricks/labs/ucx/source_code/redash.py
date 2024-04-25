import logging
from collections.abc import Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query, Dashboard
from databricks.sdk.errors.platform import NotFound

from databricks.labs.ucx.source_code.base import Fixer

logger = logging.getLogger(__name__)


class Redash:
    MIGRATED_TAG = "migrated by UCX"
    BACKUP_TAG = "backup by UCX"

    def __init__(self, fixer: Fixer, ws: WorkspaceClient):
        self._fixer = fixer
        self._ws = ws

    def fix_all_dashboards(self):
        for dashboard in self._ws.dashboards.list():
            self.fix_dashboard(dashboard)

    def fix_dashboard(self, dashboard: Dashboard):
        for query in self._get_queries_from_dashboard(dashboard):
            self._fix_query(query)

    def revert_dashboard(self, dashboard: Dashboard):
        for query in self._get_queries_from_dashboard(dashboard):
            self._revert_query(query)

    def delete_backup_dashboards(self):
        for dashboard in self._ws.dashboards.list():
            if dashboard.tags is None or self.BACKUP_TAG in dashboard.tags:
                continue

            for query in self._get_queries_from_dashboard(dashboard):
                if query.tags is None or self.BACKUP_TAG not in query.tags:
                    continue
                try:
                    self._ws.queries.delete(query.id)
                except NotFound:
                    continue

            try:
                self._ws.dashboards.delete(dashboard.id)
            except NotFound:
                pass

    def _fix_query(self, query: Query):
        assert query.id is not None
        assert query.query is not None
        if query.tags is not None and self.MIGRATED_TAG in query.tags:
            # already migrated
            return

        backup_query = self._ws.queries.create(
            data_source_id=query.data_source_id,
            description=query.description,
            name=str(query.name) + "_original",
            options=query.options.as_dict() if query.options is not None else None,
            parent=query.parent,
            query=query.query,
            run_as_role=query.run_as_role,
        )
        self._ws.api_client.do(
            "POST",
            f'/api/2.0/preview/sql/queries/{backup_query.id}',
            body={'tags': [self.BACKUP_TAG]},
        )
        new_query = self._fixer.apply(query.query)
        self._ws.api_client.do(
            "POST",
            f'/api/2.0/preview/sql/queries/{query.id}',
            body={'query': new_query, 'tags': [self.MIGRATED_TAG, f'backup: {backup_query.id}']},
        )

    def _revert_query(self, query: Query):
        assert query.id is not None
        assert query.query is not None
        if query.tags is None:
            return
        # find the backup query
        is_migrated = False
        backup_id = None
        new_tags = []
        for tag in query.tags:
            if tag == self.MIGRATED_TAG:
                is_migrated = True
                continue
            if tag.startswith("backup: "):
                backup_id = tag.split(": ")[1]
                continue
            # new tags will have all the tags except the migrated tag & backup id
            new_tags.append(tag)

        if not is_migrated:
            logger.debug(f"Query {query.id} was not migrated by UCX")
            return

        if backup_id is None:
            logger.debug(f"Cannot find backup query for query {query.id}")
            return

        original_query = self._ws.queries.get(backup_id)
        self._ws.api_client.do(
            "POST", f'/api/2.0/preview/sql/queries/{query.id}', body={'query': original_query.query, 'tags': new_tags}
        )
        self._ws.queries.delete(backup_id)

    def _get_queries_from_dashboard(self, dashboard: Dashboard) -> Iterator[Query]:
        raw = self._ws.api_client.do("GET", f"/api/2.0/preview/sql/dashboards/{dashboard.id}")
        if not isinstance(raw, dict):
            return
        for widget in raw["widgets"]:
            if "visualization" not in widget:
                continue
            viz = widget["visualization"]
            if "query" not in viz:
                continue
            yield Query.from_dict(viz["query"])
