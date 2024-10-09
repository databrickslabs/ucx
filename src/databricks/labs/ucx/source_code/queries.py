import dataclasses
import logging
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Dashboard, LegacyQuery
from databricks.sdk.service.workspace import Language

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState, LineageAtom, UsedTable
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler, DirectFsAccess
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.redash import Redash
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler

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


@dataclass
class _ReportingContext:
    linted_queries: set[str] = field(default_factory=set)
    all_problems: list[QueryProblem] = field(default_factory=list)
    all_dfsas: list[DirectFsAccess] = field(default_factory=list)
    all_tables: list[UsedTable] = field(default_factory=list)


class QueryLinter:

    def __init__(
        self,
        ws: WorkspaceClient,
        migration_index: TableMigrationIndex,
        directfs_crawler: DirectFsAccessCrawler,
        used_tables_crawler: UsedTablesCrawler,
        include_dashboard_ids: list[str] | None,
    ):
        self._ws = ws
        self._migration_index = migration_index
        self._directfs_crawler = directfs_crawler
        self._used_tables_crawler = used_tables_crawler
        self._include_dashboard_ids = include_dashboard_ids

    def refresh_report(self, sql_backend: SqlBackend, inventory_database: str):
        assessment_start = datetime.now(timezone.utc)
        context = _ReportingContext()
        self._lint_dashboards(context)
        self._lint_queries(context)
        assessment_end = datetime.now(timezone.utc)
        self._dump_problems(context, sql_backend, inventory_database)
        self._dump_dfsas(context, assessment_start, assessment_end)
        self._dump_used_tables(context, assessment_start, assessment_end)

    def _dump_problems(self, context: _ReportingContext, sql_backend: SqlBackend, inventory_database: str):
        logger.info(f"Saving {len(context.all_problems)} linting problems...")
        sql_backend.save_table(
            f'{escape_sql_identifier(inventory_database)}.query_problems',
            context.all_problems,
            QueryProblem,
            mode='overwrite',
        )

    def _dump_dfsas(self, context: _ReportingContext, assessment_start: datetime, assessment_end: datetime):
        processed_dfsas = []
        for dfsa in context.all_dfsas:
            dfsa = dataclasses.replace(
                dfsa,
                assessment_start_timestamp=assessment_start,
                assessment_end_timestamp=assessment_end,
            )
            processed_dfsas.append(dfsa)
        self._directfs_crawler.dump_all(processed_dfsas)

    def _dump_used_tables(self, context: _ReportingContext, assessment_start: datetime, assessment_end: datetime):
        processed_tables = []
        for table in context.all_tables:
            table = dataclasses.replace(
                table,
                assessment_start_timestamp=assessment_start,
                assessment_end_timestamp=assessment_end,
            )
            processed_tables.append(table)
        self._used_tables_crawler.dump_all(processed_tables)

    def _lint_dashboards(self, context: _ReportingContext):
        dashboard_ids = self._dashboard_ids_in_scope()
        logger.info(f"Running {len(dashboard_ids)} linting tasks...")
        for dashboard_id in dashboard_ids:
            dashboard = self._ws.dashboards.get(dashboard_id=dashboard_id)
            problems, dfsas, tables = self._lint_and_collect_from_dashboard(dashboard, context.linted_queries)
            context.all_problems.extend(problems)
            context.all_dfsas.extend(dfsas)
            context.all_tables.extend(tables)

    def _lint_queries(self, context: _ReportingContext):
        for query in self._queries_in_scope():
            if query.id in context.linted_queries:
                continue
            context.linted_queries.add(query.id)
            problems = self.lint_query(query)
            context.all_problems.extend(problems)
            dfsas = self.collect_dfsas_from_query("no-dashboard-id", query)
            context.all_dfsas.extend(dfsas)
            tables = self.collect_used_tables_from_query("no-dashboard-id", query)
            context.all_tables.extend(tables)

    def _dashboard_ids_in_scope(self) -> list[str]:
        if self._include_dashboard_ids is not None:  # an empty list is accepted
            return self._include_dashboard_ids
        all_dashboards = self._ws.dashboards.list()
        return [dashboard.id for dashboard in all_dashboards if dashboard.id]

    def _queries_in_scope(self):
        if self._include_dashboard_ids is not None:  # an empty list is accepted
            return []
        all_queries = self._ws.queries_legacy.list()
        return [query for query in all_queries if query.id]

    def _lint_and_collect_from_dashboard(
        self, dashboard: Dashboard, linted_queries: set[str]
    ) -> tuple[Iterable[QueryProblem], Iterable[DirectFsAccess], Iterable[UsedTable]]:
        dashboard_queries = Redash.get_queries_from_dashboard(dashboard)
        query_problems: list[QueryProblem] = []
        query_dfsas: list[DirectFsAccess] = []
        query_tables: list[UsedTable] = []
        dashboard_id = dashboard.id or "<no-id>"
        dashboard_parent = dashboard.parent or "<orphan>"
        dashboard_name = dashboard.name or "<anonymous>"
        for query in dashboard_queries:
            if query.id is None:
                continue
            if query.id in linted_queries:
                continue
            linted_queries.add(query.id)
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
            dfsas = self.collect_dfsas_from_query(dashboard_id, query)
            for dfsa in dfsas:
                atom = LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard_id,
                    other={"parent": dashboard_parent, "name": dashboard_name},
                )
                source_lineage = [atom] + dfsa.source_lineage
                query_dfsas.append(dataclasses.replace(dfsa, source_lineage=source_lineage))
            tables = self.collect_used_tables_from_query(dashboard_id, query)
            for table in tables:
                atom = LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard_id,
                    other={"parent": dashboard_parent, "name": dashboard_name},
                )
                source_lineage = [atom] + table.source_lineage
                query_tables.append(dataclasses.replace(table, source_lineage=source_lineage))
        return query_problems, query_dfsas, query_tables

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

    def collect_dfsas_from_query(self, dashboard_id: str, query: LegacyQuery) -> Iterable[DirectFsAccess]:
        if query.query is None:
            return
        ctx = LinterContext(self._migration_index, CurrentSessionState())
        collector = ctx.dfsa_collector(Language.SQL)
        source_id = f"{dashboard_id}/{query.id}"
        source_name = query.name or "<anonymous>"
        source_timestamp = self._read_timestamp(query.updated_at)
        source_lineage = [LineageAtom(object_type="QUERY", object_id=source_id, other={"name": source_name})]
        for dfsa in collector.collect_dfsas(query.query):
            yield DirectFsAccess(**asdict(dfsa)).replace_source(
                source_id=source_id, source_timestamp=source_timestamp, source_lineage=source_lineage
            )

    def collect_used_tables_from_query(self, dashboard_id: str, query: LegacyQuery) -> Iterable[UsedTable]:
        if query.query is None:
            return
        ctx = LinterContext(self._migration_index, CurrentSessionState())
        collector = ctx.tables_collector(Language.SQL)
        source_id = f"{dashboard_id}/{query.id}"
        source_name = query.name or "<anonymous>"
        source_timestamp = self._read_timestamp(query.updated_at)
        source_lineage = [LineageAtom(object_type="QUERY", object_id=source_id, other={"name": source_name})]
        for table in collector.collect_tables(query.query):
            yield UsedTable(**asdict(table)).replace_source(
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
