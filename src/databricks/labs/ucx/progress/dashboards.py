import collections
from dataclasses import replace
from functools import cached_property

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.assessment.dashboards import Dashboard, DashboardOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.queries import QueryProblem


class DashboardProgressEncoder(ProgressEncoder[Dashboard]):
    """Encoder class:Dashboard to class:History."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: DashboardOwnership,
        direct_fs_access_crawlers: list[DirectFsAccessCrawler],
        inventory_database: str,
        run_id: int,
        workspace_id: int,
        catalog: str,
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            Dashboard,
            run_id,
            workspace_id,
            catalog,
            "multiworkspace",
            "historical",
        )
        self._inventory_database = inventory_database
        self._direct_fs_access_crawlers = direct_fs_access_crawlers

    @cached_property
    def _query_problems(self) -> dict[str, list[str]]:
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

    @cached_property
    def _direct_fs_accesses(self) -> dict[str, list[str]]:
        index = collections.defaultdict(list)
        for crawler in self._direct_fs_access_crawlers:
            for direct_fs_access in crawler.snapshot():
                # The dashboard and query source lineage are added by the QueryLinter
                if len(direct_fs_access.source_lineage) < 2:
                    continue
                if direct_fs_access.source_lineage[0].object_type != "DASHBOARD":  # Note: this skips dangling queries
                    continue
                if direct_fs_access.source_lineage[1].object_type != "QUERY":
                    continue
                dashboard_id = direct_fs_access.source_lineage[0].object_id
                query_id = direct_fs_access.source_lineage[1].object_id  # <dashboard id>/<query id>
                query_name = direct_fs_access.source_lineage[1].other.get("name", "UNKNOWN")
                # Follow same failure message structure as the QueryProblem above and DirectFsAccessPyLinter deprecation
                code = "direct-filesystem-access"
                message = f"The use of direct filesystem references is deprecated: {direct_fs_access.path}"
                failure = f"[{code}] {query_name} ({query_id}) : {message}"
                index[dashboard_id].append(failure)
        return index

    def _encode_record_as_historical(self, record: Dashboard) -> Historical:
        """Encode a dashboard as a historical records.

        Failures are detected by the QueryLinter:
        - Query problems
        - Direct filesystem access by code used in query
        """
        historical = super()._encode_record_as_historical(record)
        failures = []
        failures.extend(self._query_problems.get(record.id, []))
        failures.extend(self._direct_fs_accesses.get(record.id, []))
        # TODO: Add UsedTable
        return replace(historical, failures=historical.failures + failures)
