import logging
from collections.abc import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ListQueryObjectsResponseQuery


from databricks.labs.ucx.source_code.base import DirectFsAccessInQuery
from databricks.labs.ucx.source_code.directfs_access_crawler import DirectFsAccessCrawlers
from databricks.labs.ucx.source_code.linters.directfs import DirectFsAccessSqlLinter

logger = logging.getLogger(__name__)


class QueryLinter:

    def __init__(
        self,
        ws: WorkspaceClient,
        directfs_crawlers: DirectFsAccessCrawlers,
    ):
        self._ws = ws
        self._directfs_crawlers = directfs_crawlers

    def collect_dfsas_from_queries(self) -> Iterable[DirectFsAccessInQuery]:
        dfsas = list(self._collect_dfsas_from_queries())
        self._directfs_crawlers.for_queries().append(dfsas)
        return dfsas

    def _collect_dfsas_from_queries(self) -> Iterable[DirectFsAccessInQuery]:
        queries = self._ws.queries.list()
        for query in queries:
            yield from self._collect_from_query(query)

    @classmethod
    def _collect_from_query(cls, query: ListQueryObjectsResponseQuery) -> Iterable[DirectFsAccessInQuery]:
        if query.query_text is None:
            return
        linter = DirectFsAccessSqlLinter()
        for dfsa in linter.collect_dfsas(query.query_text):
            yield DirectFsAccessInQuery(
                source_id=query.display_name or "<anonymous>",
                path=dfsa.path,
                is_read=dfsa.is_read,
                is_write=dfsa.is_write,
            )
