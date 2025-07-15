import dataclasses
import logging

from collections.abc import Iterable
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs

from databricks.labs.ucx.assessment.jobs import JobsCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    DirectFsAccess,
    UsedTable,
    LocatedAdvice,
    CurrentSessionState,
    LineageAtom,
)
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.graph import DependencyResolver, DependencyGraph, Dependency
from databricks.labs.ucx.source_code.jobs import JobProblem, WorkflowTask, WorkflowTaskContainer
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.graph_walkers import (
    LinterWalker,
    DfsaCollectorWalker,
    TablesCollectorWalker,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


logger = logging.getLogger(__name__)


class WorkflowLinter:
    def __init__(
        self,
        ws: WorkspaceClient,
        jobs_crawler: JobsCrawler,
        resolver: DependencyResolver,
        path_lookup: PathLookup,
        migration_index: TableMigrationIndex,
        directfs_crawler: DirectFsAccessCrawler,
        used_tables_crawler: UsedTablesCrawler,
    ):
        self._ws = ws
        self._jobs_crawler = jobs_crawler
        self._resolver = resolver
        self._path_lookup = path_lookup
        self._migration_index = migration_index
        self._directfs_crawler = directfs_crawler
        self._used_tables_crawler = used_tables_crawler

    def refresh_report(
        self, sql_backend: SqlBackend, inventory_database: str, /, last_run_days: int | None = None
    ) -> None:
        tasks = []

        def lint_job_limited(job_id: int) -> tuple[list[JobProblem], list[DirectFsAccess], list[UsedTable]]:
            return self.lint_job(job_id, last_run_days=last_run_days)

        for job in self._jobs_crawler.snapshot():
            try:
                job_id = int(job.job_id)
            except ValueError:
                logger.warning(f"Invalid job id: {job.job_id}")
                continue
            tasks.append(partial(lint_job_limited, job_id))
        #     TODO: Limit Scope
        logger.info(f"Running {len(tasks)} linting tasks in parallel...")
        job_results, errors = Threads.gather('linting workflows', tasks)
        job_problems: list[JobProblem] = []
        job_dfsas: list[DirectFsAccess] = []
        job_tables: list[UsedTable] = []
        for problems, dfsas, tables in job_results:
            job_problems.extend(problems)
            job_dfsas.extend(dfsas)
            job_tables.extend(tables)
        logger.info(f"Saving {len(job_problems)} linting problems...")
        sql_backend.save_table(
            f'{inventory_database}.workflow_problems',
            job_problems,
            JobProblem,
            mode='overwrite',
        )
        self._directfs_crawler.dump_all(job_dfsas)
        self._used_tables_crawler.dump_all(job_tables)
        if len(errors) > 0:
            error_messages = "\n".join([str(error) for error in errors])
            logger.warning(f"Errors occurred during linting:\n{error_messages}")

    def lint_job(
        self, job_id: int, /, last_run_days: int | None = None
    ) -> tuple[list[JobProblem], list[DirectFsAccess], list[UsedTable]]:
        try:
            job = self._ws.jobs.get(job_id)
        except NotFound:
            logger.warning(f'Could not find job: {job_id}')
            return [], [], []

        if last_run_days:
            current_day_ms = int(datetime.now().timestamp() * 1000)
            last_run_day_ms = current_day_ms - (last_run_days * 24 * 60 * 60 * 1000)
            runs = self._ws.jobs.list_runs(
                job_id=job_id,
                limit=1,
                start_time_from=last_run_day_ms,
            )
            if not runs:
                logger.warning(f'Could not find job runs in the last {last_run_days} days: {job_id}')
                return [], [], []

        problems, dfsas, tables = self._lint_job(job)
        if len(problems) > 0:
            problem_messages = "\n".join([problem.as_message() for problem in problems])
            logger.warning(f"Found job problems:\n{problem_messages}")
        return problems, dfsas, tables

    _UNKNOWN = Path('<UNKNOWN>')

    def _lint_job(self, job: jobs.Job) -> tuple[list[JobProblem], list[DirectFsAccess], list[UsedTable]]:
        problems: list[JobProblem] = []
        dfsas: list[DirectFsAccess] = []
        used_tables: list[UsedTable] = []

        assert job.job_id is not None
        assert job.settings is not None
        assert job.settings.name is not None
        assert job.settings.tasks is not None
        for task in job.settings.tasks:
            graph, advices, session_state = self._build_task_dependency_graph(task, job)
            if not advices:
                advices = self._lint_task(graph, session_state)
            for advice in advices:
                absolute_path = "UNKNOWN" if advice.has_missing_path() else advice.path.absolute().as_posix()
                job_problem = JobProblem(
                    job_id=job.job_id,
                    job_name=job.settings.name,
                    task_key=task.task_key,
                    path=absolute_path,
                    code=advice.advice.code,
                    message=advice.advice.message,
                    start_line=advice.advice.start_line,
                    start_col=advice.advice.start_col,
                    end_line=advice.advice.end_line,
                    end_col=advice.advice.end_col,
                )
                problems.append(job_problem)
            assessment_start = datetime.now(timezone.utc)
            task_dfsas = self._collect_task_dfsas(job, task, graph, session_state)
            assessment_end = datetime.now(timezone.utc)
            for dfsa in task_dfsas:
                dfsa = dfsa.replace_assessment_infos(assessment_start=assessment_start, assessment_end=assessment_end)
                dfsas.append(dfsa)
            assessment_start = datetime.now(timezone.utc)
            task_tables = self._collect_task_tables(job, task, graph, session_state)
            assessment_end = datetime.now(timezone.utc)
            for used_table in task_tables:
                used_table = used_table.replace_assessment_infos(
                    assessment_start=assessment_start,
                    assessment_end=assessment_end,
                )
                used_tables.append(used_table)

        return problems, dfsas, used_tables

    def _build_task_dependency_graph(
        self, task: jobs.Task, job: jobs.Job
    ) -> tuple[DependencyGraph, Iterable[LocatedAdvice], CurrentSessionState]:
        root_dependency: Dependency = WorkflowTask(self._ws, task, job)
        # we can load it without further preparation since the WorkflowTask is merely a wrapper
        container = root_dependency.load(self._path_lookup)
        assert isinstance(container, WorkflowTaskContainer)
        session_state = CurrentSessionState(
            data_security_mode=container.data_security_mode,
            named_parameters=container.named_parameters,
            spark_conf=container.spark_conf,
            dbr_version=container.runtime_version,
        )
        graph = DependencyGraph(root_dependency, None, self._resolver, self._path_lookup, session_state)
        problems = container.build_dependency_graph(graph)
        located_advices = [problem.as_located_advice() for problem in problems]
        return graph, located_advices, session_state

    def _lint_task(self, graph: DependencyGraph, session_state: CurrentSessionState) -> Iterable[LocatedAdvice]:
        walker = LinterWalker(graph, self._path_lookup, lambda: LinterContext(self._migration_index, session_state))
        yield from walker

    def _collect_task_dfsas(
        self,
        job: jobs.Job,
        task: jobs.Task,
        graph: DependencyGraph,
        session_state: CurrentSessionState,
    ) -> Iterable[DirectFsAccess]:
        # need to add lineage for job/task because walker doesn't register them
        job_id = str(job.job_id)
        job_name = job.settings.name if job.settings and job.settings.name else "<anonymous>"
        for dfsa in DfsaCollectorWalker(graph, self._path_lookup, session_state, self._migration_index):
            atoms = [
                LineageAtom(object_type="WORKFLOW", object_id=job_id, other={"name": job_name}),
                LineageAtom(object_type="TASK", object_id=f"{job_id}/{task.task_key}"),
            ]
            yield dataclasses.replace(dfsa, source_lineage=atoms + dfsa.source_lineage)

    def _collect_task_tables(
        self,
        job: jobs.Job,
        task: jobs.Task,
        graph: DependencyGraph,
        session_state: CurrentSessionState,
    ) -> Iterable[UsedTable]:
        # need to add lineage for job/task because walker doesn't register them
        job_id = str(job.job_id)
        job_name = job.settings.name if job.settings and job.settings.name else "<anonymous>"
        for used_table in TablesCollectorWalker(graph, self._path_lookup, session_state, self._migration_index):
            atoms = [
                LineageAtom(object_type="WORKFLOW", object_id=job_id, other={"name": job_name}),
                LineageAtom(object_type="TASK", object_id=f"{job_id}/{task.task_key}"),
            ]
            yield dataclasses.replace(used_table, source_lineage=atoms + used_table.source_lineage)
