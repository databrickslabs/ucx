import logging
import time
from collections.abc import Iterable
from dataclasses import asdict
from datetime import datetime

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
        assessment_start = int(time.mktime(time.gmtime()))
        queries_dfsas = self._collect_dfsas_from_queries()
        assessment_end = int(time.mktime(time.gmtime()))
        dfsas: list[DirectFsAccessInQuery] = []
        for dfsa in queries_dfsas:
            dfsas.append(
                dfsa.replace_assessment_infos(assessment_start=assessment_start, assessment_end=assessment_end)
            )
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
        source_id = query.display_name or "<anonymous>"
        source_timestamp = (
            -1 if query.update_time is None else int(datetime.fromisoformat(query.update_time).timestamp())
        )
        for dfsa in linter.collect_dfsas(query.query_text):
            yield DirectFsAccessInQuery(**asdict(dfsa)).replace_source(
                source_id=source_id, source_timestamp=source_timestamp
            )
