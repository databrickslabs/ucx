import collections
import logging
from collections.abc import Iterable
from dataclasses import replace

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.assessment.jobs import JobInfo, JobOwnership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.jobs import JobProblem


logger = logging.getLogger(__name__)
JobIdToFailuresType = dict[str, list[str]]  # dict[<job id>, list[<failure message>]]


class JobsProgressEncoder(ProgressEncoder[JobInfo]):
    """Encoder class:Job to class:History."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: JobOwnership,
        direct_fs_access_crawlers: list[DirectFsAccessCrawler],
        inventory_database: str,
        run_id: int,
        workspace_id: int,
        catalog: str,
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            JobInfo,
            run_id,
            workspace_id,
            catalog,
            "multiworkspace",
            "historical",
        )
        self._direct_fs_access_crawlers = direct_fs_access_crawlers
        self._inventory_database = inventory_database

    def append_inventory_snapshot(self, snapshot: Iterable[JobInfo]) -> None:
        job_problems = self._get_job_problems()
        dfsas = self._get_direct_filesystem_accesses()
        history_records = []
        for record in snapshot:
            history_record = self._encode_job_as_historical(record, job_problems, dfsas)
            history_records.append(history_record)
        logger.debug(f"Appending {len(history_records)} {self._klass} table record(s) to history.")
        # The mode is 'append'. This is documented as conflict-free.
        self._sql_backend.save_table(escape_sql_identifier(self.full_name), history_records, Historical, mode="append")

    def _get_job_problems(self) -> JobIdToFailuresType:
        index = collections.defaultdict(list)
        for row in self._sql_backend.fetch(
            'SELECT * FROM workflow_problems',
            catalog='hive_metastore',
            schema=self._inventory_database,
        ):
            job_problem = JobProblem(**row.asDict())
            failure = f'{job_problem.code}: {job_problem.task_key} task: {job_problem.path}: {job_problem.message}'
            index[str(job_problem.job_id)].append(failure)
        return index

    def _get_direct_filesystem_accesses(self) -> JobIdToFailuresType:
        index = collections.defaultdict(list)
        for crawler in self._direct_fs_access_crawlers:
            for direct_fs_access in crawler.snapshot():
                # The workflow and task source lineage are added by the WorkflowLinter
                if len(direct_fs_access.source_lineage) < 2:
                    continue
                if direct_fs_access.source_lineage[0].object_type != "WORKFLOW":
                    continue
                if direct_fs_access.source_lineage[1].object_type != "TASK":
                    continue
                job_id = direct_fs_access.source_lineage[0].object_id
                task_key = direct_fs_access.source_lineage[1].object_id  # <job id>/<task key>
                # Follow same failure message structure as the JobProblems above and DirectFsAccessPyLinter deprecation
                code = "direct-filesystem-access"
                message = f"The use of direct filesystem references is deprecated: {direct_fs_access.path}"
                failure = f"{code}: {task_key} task: {direct_fs_access.source_id}: {message}"
                index[job_id].append(failure)
        return index

    def _encode_job_as_historical(
        self,
        record: JobInfo,
        job_problems: JobIdToFailuresType,
        dfsas: JobIdToFailuresType,
    ) -> Historical:
        """Encode a job as a historical records.

        Failures are detected by the WorkflowLinter:
        - Job problems
        - Direct filesystem access by code used in job
        """
        historical = super()._encode_record_as_historical(record)
        failures = []
        failures.extend(job_problems.get(record.job_id, []))
        failures.extend(dfsas.get(record.job_id, []))
        return replace(historical, failures=historical.failures + failures)
