import logging
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ListQueryObjectsResponseQuery
from databricks.sdk.service.workspace import Language

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler, DirectFsAccessInQuery, LineageAtom
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.directfs import DirectFsAccessSqlLinter

logger = logging.getLogger(__name__)


@dataclass
class QueryProblem:
    query_parent: str
    query_name: str
    code: str
    message: str


class QueryLinter:

    def __init__(
        self,
        ws: WorkspaceClient,
        migration_index: TableMigrationIndex,
        directfs_crawler: DirectFsAccessCrawler,
    ):
        self._ws = ws
        self._migration_index = migration_index
        self._directfs_crawler = directfs_crawler

    def refresh_report(self, sql_backend: SqlBackend, inventory_database: str):
        assessment_start = datetime.now()
        all_queries = list(self._ws.queries.list())
        logger.info(f"Running {len(all_queries)} linting tasks...")
        query_problems: list[QueryProblem] = []
        query_dfsas: list[DirectFsAccessInQuery] = []
        for query in all_queries:
            problems = self.lint_query(query)
            query_problems.extend(problems)
            dfsas = self.collect_dfsas_from_query(query)
            assessment_end = datetime.now()
            for dfsa in dfsas:
                query_dfsas.append(
                    dfsa.replace_assessment_infos(assessment_start=assessment_start, assessment_end=assessment_end)
                )
        logger.info(f"Saving {len(query_problems)} linting problems...")
        sql_backend.save_table(
            f'{inventory_database}.query_problems',
            query_problems,
            QueryProblem,
            mode='overwrite',
        )
        self._directfs_crawler.append(query_dfsas)

    def lint_query(self, query: ListQueryObjectsResponseQuery) -> Iterable[QueryProblem]:
        if not query.query_text:
            return
        ctx = LinterContext(self._migration_index, CurrentSessionState())
        linter = ctx.linter(Language.SQL)
        for advice in linter.lint(query.query_text):
            yield QueryProblem(
                query_parent="",  # TODO get from sdk once 'path_parent' in query field is added to ListQueryObjectsResponseQuery
                query_name=query.display_name or "unnamed query",
                code=advice.code,
                message=advice.message,
            )

    @classmethod
    def collect_dfsas_from_query(cls, query: ListQueryObjectsResponseQuery) -> Iterable[DirectFsAccessInQuery]:
        if query.query_text is None:
            return
        linter = DirectFsAccessSqlLinter()
        source_id = query.id or "no id"
        source_name = query.display_name or "<anonymous>"
        source_timestamp = cls._read_timestamp(query.update_time)
        source_lineage = [LineageAtom(object_type="QUERY", object_id=source_id, other={"query_name": source_name})]
        for dfsa in linter.collect_dfsas(query.query_text):
            yield DirectFsAccessInQuery(**asdict(dfsa)).replace_source(
                source_id=source_id, source_timestamp=source_timestamp, source_lineage=source_lineage
            )

    @classmethod
    def _read_timestamp(cls, timestamp: str | None) -> datetime:
        if timestamp is not None:
            methods = [
                datetime.fromisoformat,
                lambda s: datetime.fromisoformat(s[:-1]),  # ipython breaks on final 'Z'
            ]
            for method in methods:
                try:
                    return method(timestamp)
                except ValueError:
                    pass
        return datetime.now()
