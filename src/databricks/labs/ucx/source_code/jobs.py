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

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.files import LocalFile
from databricks.labs.ucx.source_code.graph import (
    DependencyGraph,
    SourceContainer,
    DependencyProblem,
    Dependency,
    WrappingLoader,
    DependencyResolver,
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

    def _register_libraries(self, graph: DependencyGraph):
        if not self._task.libraries:
            return
        for library in self._task.libraries:
            yield from self._lint_library(library, graph)

    def _lint_library(self, library: compute.Library, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if library.pypi:
            # TODO: https://github.com/databrickslabs/ucx/issues/1642
            maybe = graph.register_library(library.pypi.package)
            if maybe.problems:
                yield from maybe.problems
        if library.jar:
            # TODO: https://github.com/databrickslabs/ucx/issues/1641
            yield DependencyProblem('not-yet-implemented', 'Jar library is not yet implemented')
        if library.egg:
            # TODO: https://github.com/databrickslabs/ucx/issues/1643
            maybe = graph.register_library(library.egg)
            if maybe.problems:
                yield from maybe.problems
        if library.whl:
            # TODO: download the wheel somewhere local and add it to "virtual sys.path" via graph.path_lookup.push_path
            # TODO: https://github.com/databrickslabs/ucx/issues/1640
            maybe = graph.register_library(library.whl)
            if maybe.problems:
                yield from maybe.problems
        if library.requirements:
            # TODO: download and add every entry via graph.register_library
            # TODO: https://github.com/databrickslabs/ucx/issues/1644
            yield DependencyProblem('not-yet-implemented', 'Requirements library is not yet implemented')

    def _register_notebook(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.notebook_task:
            return []
        notebook_path = self._task.notebook_task.notebook_path
        logger.info(f'Disovering {self._task.task_key} entrypoint: {notebook_path}')
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
        for job in self._ws.jobs.list():
            tasks.append(functools.partial(self.lint_job, job.job_id))
        problems: list[JobProblem] = []
        for batch in Threads.strict('linting workflows', tasks):
            problems.extend(batch)
        sql_backend.save_table(f'{inventory_database}.workflow_problems', problems, JobProblem, mode='overwrite')

    def lint_job(self, job_id: int) -> list[JobProblem]:
        try:
            job = self._ws.jobs.get(job_id)
            return list(self._lint_job(job))
        except NotFound:
            logger.warning(f'Could not find job: {job_id}')
            return []

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
