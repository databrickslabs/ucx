import dataclasses
import logging
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Dashboard, LegacyQuery
from databricks.sdk.service.workspace import Language

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler, DirectFsAccessInQuery, LineageAtom
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.directfs import DirectFsAccessSqlLinter
from databricks.labs.ucx.source_code.redash import Redash

logger = logging.getLogger(__name__)


@dataclass
class QueryProblem:
    dashboard_id: str
    dashboard_parent: str
    dashboard_name: str
    query_id: str
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
        linted_queries: set[LegacyQuery] = set()
        all_dashboards = list(self._ws.dashboards.list())
        logger.info(f"Running {len(all_dashboards)} linting tasks...")
        all_problems: list[QueryProblem] = []
        all_dfsas: list[DirectFsAccessInQuery] = []
        for dashboard in all_dashboards:
            problems, dfsas = self._lint_and_collect_from_dashboard(dashboard, linted_queries)
            all_problems.extend(problems)
            assessment_end = datetime.now()
            for dfsa in dfsas:
                all_dfsas.append(
                    dataclasses.replace(
                        dfsa, assessment_start_timestamp=assessment_start, assessment_end_timestamp=assessment_end
                    )
                )
        logger.info(f"Saving {len(all_problems)} linting problems...")
        sql_backend.save_table(
            f'{escape_sql_identifier(inventory_database)}.query_problems',
            all_problems,
            QueryProblem,
            mode='overwrite',
        )
        self._directfs_crawler.dump_all(all_dfsas)

    def _lint_and_collect_from_dashboard(
        self, dashboard: Dashboard, linted_queries: set[LegacyQuery]
    ) -> tuple[Iterable[QueryProblem], Iterable[DirectFsAccessInQuery]]:
        dashboard_queries = Redash.get_queries_from_dashboard(dashboard)
        query_problems: list[QueryProblem] = []
        query_dfsas: list[DirectFsAccessInQuery] = []
        dashboard_id = dashboard.id or "<no-id>"
        dashboard_parent = dashboard.parent or "<orphan>"
        dashboard_name = dashboard.name or "<anonymous>"
        for query in dashboard_queries:
            if query in linted_queries:
                continue
            linted_queries.add(query)
            problems = self.lint_query(query)
            for problem in problems:
                query_problems.append(
                    dataclasses.replace(
                        problem,
                        dashboard_id=dashboard_id,
                        dashboard_parent=dashboard_parent,
                        dashboard_name=dashboard_name,
                    )
                )
            dfsas = self.collect_dfsas_from_query(query)
            for dfsa in dfsas:
                atom = LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard_id,
                    other={"parent": dashboard_parent, "name": dashboard_name},
                )
                source_lineage = [atom] + dfsa.source_lineage
                query_dfsas.append(dataclasses.replace(dfsa, source_lineage=source_lineage))
        return query_problems, query_dfsas

    def lint_query(self, query: LegacyQuery) -> Iterable[QueryProblem]:
        if not query.query:
            return
        ctx = LinterContext(self._migration_index, CurrentSessionState())
        linter = ctx.linter(Language.SQL)
        query_id = query.id or "<no-id>"
        query_parent = query.parent or "<orphan>"
        query_name = query.name or "<anonymous>"
        for advice in linter.lint(query.query):
            yield QueryProblem(
                dashboard_id="",
                dashboard_parent="",
                dashboard_name="",
                query_id=query_id,
                query_parent=query_parent,
                query_name=query_name,
                code=advice.code,
                message=advice.message,
            )

    @classmethod
    def collect_dfsas_from_query(cls, query: LegacyQuery) -> Iterable[DirectFsAccessInQuery]:
        if query.query is None:
            return
        linter = DirectFsAccessSqlLinter()
        source_id = query.id or "no id"
        source_name = query.name or "<anonymous>"
        source_timestamp = cls._read_timestamp(query.updated_at)
        source_lineage = [LineageAtom(object_type="QUERY", object_id=source_id, other={"query_name": source_name})]
        for dfsa in linter.collect_dfsas(query.query):
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
