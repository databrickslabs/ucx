import dataclasses
import functools
import logging

from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs

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
    LintingWalker,
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
        resolver: DependencyResolver,
        path_lookup: PathLookup,
        migration_index: TableMigrationIndex,
        directfs_crawler: DirectFsAccessCrawler,
        used_tables_crawler: UsedTablesCrawler,
        include_job_ids: list[int] | None = None,
        debug_listing_upper_limit: int | None = None,
    ):
        self._ws = ws
        self._resolver = resolver
        self._path_lookup = path_lookup
        self._migration_index = migration_index
        self._directfs_crawler = directfs_crawler
        self._used_tables_crawler = used_tables_crawler
        self._include_job_ids = include_job_ids
        self._debug_listing_upper_limit = debug_listing_upper_limit

    def refresh_report(self, sql_backend: SqlBackend, inventory_database: str) -> None:
        tasks = []
        items_listed = 0
        for job in self._ws.jobs.list():
            if self._include_job_ids is not None and job.job_id not in self._include_job_ids:
                logger.info(f"Skipping job_id={job.job_id}")
                continue
            if self._debug_listing_upper_limit is not None and items_listed >= self._debug_listing_upper_limit:
                logger.warning(f"Debug listing limit reached: {self._debug_listing_upper_limit}")
                break
            if job.settings is not None and job.settings.name is not None:
                logger.info(f"Found job_id={job.job_id}: {job.settings.name}")
            tasks.append(functools.partial(self.lint_job, job.job_id))
            items_listed += 1
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

    def lint_job(self, job_id: int) -> tuple[list[JobProblem], list[DirectFsAccess], list[UsedTable]]:
        try:
            job = self._ws.jobs.get(job_id)
        except NotFound:
            logger.warning(f'Could not find job: {job_id}')
            return ([], [], [])

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
        linted_paths: set[Path] = set()
        for task in job.settings.tasks:
            graph, advices, session_state = self._build_task_dependency_graph(task, job)
            if not advices:
                advices = self._lint_task(task, graph, session_state, linted_paths)
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

    def _lint_task(
        self,
        task: jobs.Task,
        graph: DependencyGraph,
        session_state: CurrentSessionState,
        linted_paths: set[Path],
    ) -> Iterable[LocatedAdvice]:
        walker = LintingWalker(
            graph,
            linted_paths,
            self._path_lookup,
            task.task_key,
            session_state,
            lambda: LinterContext(self._migration_index, session_state),
        )
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
        for dfsa in DfsaCollectorWalker(graph, set(), self._path_lookup, session_state, self._migration_index):
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
        for used_table in TablesCollectorWalker(graph, set(), self._path_lookup, session_state, self._migration_index):
            atoms = [
                LineageAtom(object_type="WORKFLOW", object_id=job_id, other={"name": job_name}),
                LineageAtom(object_type="TASK", object_id=f"{job_id}/{task.task_key}"),
            ]
            yield dataclasses.replace(used_table, source_lineage=atoms + used_table.source_lineage)
