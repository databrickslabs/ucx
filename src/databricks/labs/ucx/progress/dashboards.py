import collections
import logging
from collections.abc import Iterable
from dataclasses import replace

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.assessment.dashboards import Dashboard, DashboardOwnership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.base import UsedTable
from databricks.labs.ucx.source_code.linters.queries import QueryProblem
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


logger = logging.getLogger(__name__)


DashboardIdToFailuresType = dict[str, list[str]]  # dict[<dashboard id>, list[<failure message>]]


class DashboardProgressEncoder(ProgressEncoder[Dashboard]):
    """Encoder class:Dashboard to class:History."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: DashboardOwnership,
        *,
        used_tables_crawlers: list[UsedTablesCrawler],
        inventory_database: str,
        job_run_id: int,
        workspace_id: int,
        catalog: str,
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            Dashboard,
            job_run_id,
            workspace_id,
            catalog,
            "multiworkspace",
            "historical",
        )
        self._inventory_database = inventory_database
        self._used_tables_crawlers = used_tables_crawlers

    def append_inventory_snapshot(self, snapshot: Iterable[Dashboard]) -> None:
        query_problems = self._get_query_problems()
        table_failures = self._get_tables_failures()
        history_records = []
        for record in snapshot:
            history_record = self._encode_dashboard_as_historical(record, query_problems, table_failures)
            history_records.append(history_record)
        logger.debug(f"Appending {len(history_records)} {self._klass} table record(s) to history.")
        # The mode is 'append'. This is documented as conflict-free.
        self._sql_backend.save_table(escape_sql_identifier(self.full_name), history_records, Historical, mode="append")

    def _get_query_problems(self) -> DashboardIdToFailuresType:
        index = collections.defaultdict(list)
        for row in self._sql_backend.fetch(
            'SELECT * FROM query_problems',
            catalog='hive_metastore',
            schema=self._inventory_database,
        ):
            problem = QueryProblem(**row.asDict())
            failure = (
                f'[{problem.code}] {problem.query_name} ({problem.dashboard_id}/{problem.query_id}) : {problem.message}'
            )
            index[problem.dashboard_id].append(failure)
        return index

    def _get_used_tables(self) -> dict[str, list[UsedTable]]:
        index = collections.defaultdict(list)
        for crawler in self._used_tables_crawlers:
            for used_table in crawler.snapshot():
                # The dashboard and query source lineage are added by the QueryLinter
                if len(used_table.source_lineage) < 2:
                    continue
                if used_table.source_lineage[0].object_type != "DASHBOARD":  # Note: this skips dangling queries
                    continue
                if used_table.source_lineage[1].object_type != "QUERY":
                    continue
                dashboard_id = used_table.source_lineage[0].object_id
                index[dashboard_id].append(used_table)
        return index

    def _get_tables_failures(self) -> DashboardIdToFailuresType:
        table_failures = {}
        for row in self._sql_backend.fetch(
            "SELECT * FROM objects_snapshot WHERE object_type = 'Table'",
            catalog=self._catalog,
            schema=self._schema,
        ):
            historical = Historical(**row.asDict())
            table = Table.from_historical_data(historical.data)
            table_failures[table.full_name] = historical.failures
        index = collections.defaultdict(list)
        used_tables = self._get_used_tables()
        for dashboard_id, used_tables_in_dashboard in used_tables.items():
            for used_table in used_tables_in_dashboard:
                for failure in table_failures.get(used_table.full_name, []):
                    index[dashboard_id].append(f"{failure}: {used_table.full_name}")
        return index

    def _encode_dashboard_as_historical(
        self,
        record: Dashboard,
        query_problems: DashboardIdToFailuresType,
        tables_failures: DashboardIdToFailuresType,
    ) -> Historical:
        """Encode a dashboard as a historical records.

        Failures are detected by the QueryLinter:
        - Query problems
        - Direct filesystem access by code used in query
        - Hive metastore tables
        """
        historical = super()._encode_record_as_historical(record)
        failures = []
        failures.extend(query_problems.get(record.id, []))
        failures.extend(tables_failures.get(record.id, []))
        return replace(historical, failures=historical.failures + failures)
