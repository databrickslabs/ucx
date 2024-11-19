import dataclasses
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
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
        sql_backend: SqlBackend,
        inventory_database: str,
        migration_index: TableMigrationIndex,
        directfs_crawler: DirectFsAccessCrawler,
        used_tables_crawler: UsedTablesCrawler,
        include_dashboard_ids: list[str] | None,
        debug_listing_upper_limit: int | None = None,
    ):
        self._ws = ws
        self._sql_backend = sql_backend
        self._migration_index = migration_index
        self._directfs_crawler = directfs_crawler
        self._used_tables_crawler = used_tables_crawler
        self._include_dashboard_ids = include_dashboard_ids
        self._debug_listing_upper_limit = debug_listing_upper_limit

        self._catalog = "hive_metastore"
        self._schema = inventory_database
        self._table = "query_problems"

    @property
    def _full_name(self) -> str:
        """Generates the full name of the table.

        Returns:
            str: The full table name.
        """
        return f"{self._catalog}.{self._schema}.{self._table}"

    def refresh_report(self) -> None:
        assessment_start = datetime.now(timezone.utc)
        context = _ReportingContext()
        self._lint_dashboards(context)
        self._lint_queries(context)
        assessment_end = datetime.now(timezone.utc)
        self._dump_problems(context.all_problems)
        self._dump_dfsas(context.all_dfsas, assessment_start, assessment_end)
        self._dump_used_tables(context.all_tables, assessment_start, assessment_end)

    def _dump_problems(self, problems: Sequence[QueryProblem]) -> None:
        logger.info(f"Saving {len(problems)} linting problems...")
        self._sql_backend.save_table(
            escape_sql_identifier(self._full_name),
            problems,
            QueryProblem,
            mode='overwrite',
        )

    def _dump_dfsas(
        self,
        dfsas: Sequence[DirectFsAccess],
        assessment_start: datetime,
        assessment_end: datetime,
    ) -> None:
        processed_dfsas = []
        for dfsa in dfsas:
            dfsa = dataclasses.replace(
                dfsa,
                assessment_start_timestamp=assessment_start,
                assessment_end_timestamp=assessment_end,
            )
            processed_dfsas.append(dfsa)
        self._directfs_crawler.dump_all(processed_dfsas)

    def _dump_used_tables(
        self,
        used_tables: Sequence[UsedTable],
        assessment_start: datetime,
        assessment_end: datetime,
    ) -> None:
        processed_tables = []
        for used_table in used_tables:
            used_table = dataclasses.replace(
                used_table,
                assessment_start_timestamp=assessment_start,
                assessment_end_timestamp=assessment_end,
            )
            processed_tables.append(used_table)
        self._used_tables_crawler.dump_all(processed_tables)

    def _lint_dashboards(self, context: _ReportingContext) -> None:
        for dashboard_id in self._dashboard_ids_in_scope():
            dashboard = self._ws.dashboards.get(dashboard_id=dashboard_id)
            logger.info(f"Linting dashboard_id={dashboard_id}: {dashboard.name}")
            problems, dfsas, tables = self._lint_and_collect_from_dashboard(dashboard, context.linted_queries)
            context.all_problems.extend(problems)
            context.all_dfsas.extend(dfsas)
            context.all_tables.extend(tables)

    def _lint_queries(self, context: _ReportingContext) -> None:
        for query in self._queries_in_scope():
            assert query.id is not None
            if query.id in context.linted_queries:
                continue
            logger.info(f"Linting query_id={query.id}: {query.name}")
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
        items_listed = 0
        dashboard_ids = []
        # redash APIs are very slow to paginate, especially for large number of dashboards, so we limit the listing
        # to a small number of items in debug mode for the assessment workflow just to complete.
        for dashboard in self._ws.dashboards.list():
            if self._debug_listing_upper_limit is not None and items_listed >= self._debug_listing_upper_limit:
                logger.warning(f"Debug listing limit reached: {self._debug_listing_upper_limit}")
                break
            if dashboard.id is None:
                continue
            dashboard_ids.append(dashboard.id)
            items_listed += 1
        return dashboard_ids

    def _queries_in_scope(self) -> list[LegacyQuery]:
        if self._include_dashboard_ids is not None:  # an empty list is accepted
            return []
        items_listed = 0
        legacy_queries = []
        for query in self._ws.queries_legacy.list():
            if self._debug_listing_upper_limit is not None and items_listed >= self._debug_listing_upper_limit:
                logger.warning(f"Debug listing limit reached: {self._debug_listing_upper_limit}")
                break
            legacy_queries.append(query)
            items_listed += 1
        return legacy_queries

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
            yield dfsa.replace_source(
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
            yield table.replace_source(
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
