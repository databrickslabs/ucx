import collections
from dataclasses import replace
from functools import cached_property

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.assessment.jobs import JobInfo, JobOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.jobs import JobProblem


class JobsProgressEncoder(ProgressEncoder[JobInfo]):

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: JobOwnership,
        direct_fs_access_crawlers: list[DirectFsAccessCrawler],
        inventory_database: str,
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            JobInfo,
            run_id,
            workspace_id,
            catalog,
            schema,
            table,
        )
        self._direct_fs_access_crawlers = direct_fs_access_crawlers
        self._inventory_database = inventory_database

    @cached_property
    def _job_problems(self) -> dict[int, list[str]]:
        index = collections.defaultdict(list)
        for row in self._sql_backend.fetch(
            'SELECT * FROM workflow_problems',
            catalog='hive_metastore',
            schema=self._inventory_database,
        ):
            job_problem = JobProblem(**row.asDict())
            failure = f'{job_problem.code}: {job_problem.task_key} task: {job_problem.path}: {job_problem.message}'
            index[job_problem.job_id].append(failure)
        return index

    @cached_property
    def _direct_fs_accesses(self) -> dict[str, list[str]]:
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
                failure = f"Direct file system access by '{task_key}' in '{direct_fs_access.source_id} to '{direct_fs_access.path}'"
                index[job_id].append(failure)
        return index

    def _encode_record_as_historical(self, record: JobInfo) -> Historical:
        historical = super()._encode_record_as_historical(record)
        failures = []
        failures.extend(self._job_problems.get(int(record.job_id), []))
        failures.extend(self._direct_fs_accesses.get(record.job_id, []))
        return replace(historical, failures=historical.failures + failures)
