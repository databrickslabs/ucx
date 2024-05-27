import functools
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.files import LocalFile
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyProblem,
    DependencyResolver,
    SourceContainer,
    WrappingLoader,
)
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.sources import Notebook, NotebookLinter, FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


@dataclass
class JobProblem:
    job_id: int
    job_name: str
    task_key: str
    path: str
    code: str
    message: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int

    def as_message(self) -> str:
        message = f"{self.path}:{self.start_line} [{self.code}] {self.message}"
        return message


class WorkflowTask(Dependency):
    def __init__(self, ws: WorkspaceClient, task: jobs.Task, job: jobs.Job):
        loader = WrappingLoader(WorkflowTaskContainer(ws, task))
        super().__init__(loader, Path(f'/jobs/{task.task_key}'))
        self._task = task
        self._job = job

    def load(self, path_lookup: PathLookup) -> SourceContainer | None:
        return self._loader.load_dependency(path_lookup, self)

    def __repr__(self):
        return f'WorkflowTask<{self._task.task_key} of {self._job.settings.name}>'


class WorkflowTaskContainer(SourceContainer):
    def __init__(self, ws: WorkspaceClient, task: jobs.Task):
        self._task = task
        self._ws = ws

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        return list(self._register_task_dependencies(parent))

    def _register_task_dependencies(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        yield from self._register_libraries(graph)
        yield from self._register_notebook(graph)
        yield from self._register_spark_python_task(graph)
        yield from self._register_python_wheel_task(graph)
        yield from self._register_spark_jar_task(graph)
        yield from self._register_run_job_task(graph)
        yield from self._register_pipeline_task(graph)
        yield from self._register_existing_cluster_id(graph)
        yield from self._register_spark_submit_task(graph)

    def _register_libraries(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.libraries:
            return
        for library in self._task.libraries:
            yield from self._register_library(graph, library)

    def _register_library(self, graph: DependencyGraph, library: compute.Library) -> Iterable[DependencyProblem]:
        if library.pypi:
            problems = graph.register_library(library.pypi.package)
            if problems:
                yield from problems
        if library.egg:
            # TODO: https://github.com/databrickslabs/ucx/issues/1643
            yield DependencyProblem("not-yet-implemented", "Egg library is not yet implemented")
        if library.whl:
            # TODO: download the wheel somewhere local and add it to "virtual sys.path" via graph.path_lookup.push_path
            # TODO: https://github.com/databrickslabs/ucx/issues/1640
            yield DependencyProblem("not-yet-implemented", "Wheel library is not yet implemented")
        if library.requirements:  # https://pip.pypa.io/en/stable/reference/requirements-file-format/
            logger.info(f"Registering libraries from {library.requirements}")
            with self._ws.workspace.download(library.requirements, format=ExportFormat.AUTO) as remote_file:
                contents = remote_file.read().decode()
                for requirement in contents.splitlines():
                    clean_requirement = requirement.replace(" ", "")  # requirements.txt may contain spaces
                    if clean_requirement.startswith("-r"):
                        logger.warning(f"References to other requirements file is not supported: {requirement}")
                        continue
                    if clean_requirement.startswith("-c"):
                        logger.warning(f"References to constrains file is not supported: {requirement}")
                        continue
                    yield from graph.register_library(clean_requirement)
        if library.jar:
            # TODO: https://github.com/databrickslabs/ucx/issues/1641
            yield DependencyProblem('not-yet-implemented', 'Jar library is not yet implemented')

    def _register_notebook(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.notebook_task:
            return []
        notebook_path = self._task.notebook_task.notebook_path
        logger.info(f'Discovering {self._task.task_key} entrypoint: {notebook_path}')
        path = WorkspacePath(self._ws, notebook_path)
        return graph.register_notebook(path)

    def _register_spark_python_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.spark_python_task:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1639
        yield DependencyProblem('not-yet-implemented', 'Spark Python task is not yet implemented')

    def _register_python_wheel_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.python_wheel_task:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1640
        yield DependencyProblem('not-yet-implemented', 'Python wheel task is not yet implemented')

    def _register_spark_jar_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.spark_jar_task:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1641
        yield DependencyProblem('not-yet-implemented', 'Spark Jar task is not yet implemented')

    def _register_run_job_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.run_job_task:
            return
        # TODO: it's not clear how to terminate the graph
        yield DependencyProblem('not-yet-implemented', 'Run job task is not yet implemented')

    def _register_pipeline_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.pipeline_task:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1638
        yield DependencyProblem('not-yet-implemented', 'Pipeline task is not yet implemented')

    def _register_existing_cluster_id(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.existing_cluster_id:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1637
        # load libraries installed on the referred cluster
        yield DependencyProblem('not-yet-implemented', 'Existing cluster id is not yet implemented')

    def _register_spark_submit_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.spark_submit_task:
            return
        yield DependencyProblem('not-yet-implemented', 'Spark submit task is not yet implemented')


class WorkflowLinter:
    def __init__(
        self,
        ws: WorkspaceClient,
        resolver: DependencyResolver,
        path_lookup: PathLookup,
        migration_index: MigrationIndex,
    ):
        self._ws = ws
        self._resolver = resolver
        self._path_lookup = path_lookup
        self._migration_index = migration_index

    def refresh_report(self, sql_backend: SqlBackend, inventory_database: str):
        tasks = []
        all_jobs = list(self._ws.jobs.list())
        logger.info(f"Preparing {len(all_jobs)} linting jobs...")
        for job in all_jobs:
            tasks.append(functools.partial(self.lint_job, job.job_id))
        problems: list[JobProblem] = []
        logger.info(f"Running {tasks} linting tasks in parallel...")
        for batch in Threads.strict('linting workflows', tasks):
            problems.extend(batch)
        logger.info(f"Saving {len(problems)} linting problems...")
        sql_backend.save_table(f'{inventory_database}.workflow_problems', problems, JobProblem, mode='overwrite')

    def lint_job(self, job_id: int) -> list[JobProblem]:
        try:
            job = self._ws.jobs.get(job_id)
        except NotFound:
            logger.warning(f'Could not find job: {job_id}')
            return []

        problems = self._lint_job(job)
        if len(problems) > 0:
            problem_messages = "\n".join([problem.as_message() for problem in problems])
            logger.warning(f"Found job problems:\n{problem_messages}")
        return problems

    _UNKNOWN = Path('<UNKNOWN>')

    def _lint_job(self, job: jobs.Job) -> list[JobProblem]:
        problems: list[JobProblem] = []
        assert job.job_id is not None
        assert job.settings is not None
        assert job.settings.name is not None
        assert job.settings.tasks is not None
        for task in job.settings.tasks:
            for path, advice in self._lint_task(task, job):
                absolute_path = path.absolute().as_posix() if path != self._UNKNOWN else 'UNKNOWN'
                job_problem = JobProblem(
                    job_id=job.job_id,
                    job_name=job.settings.name,
                    task_key=task.task_key,
                    path=absolute_path,
                    code=advice.code,
                    message=advice.message,
                    start_line=advice.start_line,
                    start_col=advice.start_col,
                    end_line=advice.end_line,
                    end_col=advice.end_col,
                )
                problems.append(job_problem)
        return problems

    def _lint_task(self, task: jobs.Task, job: jobs.Job):
        dependency: Dependency = WorkflowTask(self._ws, task, job)
        graph = DependencyGraph(dependency, None, self._resolver, self._path_lookup)
        container = dependency.load(self._path_lookup)
        assert container is not None  # because we just created it
        problems = container.build_dependency_graph(graph)
        if problems:
            for problem in problems:
                source_path = self._UNKNOWN if problem.is_path_missing() else problem.source_path
                yield source_path, problem
            return
        session_state = CurrentSessionState()
        state = Languages(self._migration_index, session_state)
        for dependency in graph.all_dependencies:
            logger.info(f'Linting {task.task_key} dependency: {dependency}')
            container = dependency.load(graph.path_lookup)
            if not container:
                continue
            if isinstance(container, Notebook):
                yield from self._lint_notebook(container, state)
            if isinstance(container, LocalFile):
                yield from self._lint_file(container, state)

    def _lint_file(self, file: LocalFile, state: Languages):
        linter = FileLinter(state, file.path)
        for advice in linter.lint():
            yield file.path, advice

    def _lint_notebook(self, notebook: Notebook, state: Languages):
        linter = NotebookLinter(state, notebook)
        for advice in linter.lint():
            yield notebook.path, advice
