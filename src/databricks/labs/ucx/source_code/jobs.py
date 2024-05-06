from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.base import Advice, CurrentSessionState
from databricks.labs.ucx.source_code.files import LocalFile
from databricks.labs.ucx.source_code.graph import DependencyGraphBuilder
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.sources import Notebook, NotebookLinter, FileLinter
from databricks.labs.ucx.source_code.site_packages import SitePackageContainer
from databricks.labs.ucx.source_code.whitelist import Whitelist


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


class WorkflowLinter:
    def __init__(
        self,
        ws: WorkspaceClient,
        builder: DependencyGraphBuilder,
        migration_index: MigrationIndex,
        whitelist: Whitelist,
    ):
        self._ws = ws
        self._builder = builder
        self._migration_index = migration_index
        self._whitelist = whitelist

    def lint_job(self, job_id: int):
        job = self._ws.jobs.get(job_id)
        return list(self._lint_job(job))

    def _lint_job(self, job: jobs.Job) -> list[JobProblem]:
        problems: list[JobProblem] = []
        assert job.job_id is not None
        assert job.settings is not None
        assert job.settings.name is not None
        assert job.settings.tasks is not None
        for task in job.settings.tasks:
            for path, advice in self._lint_task(task):
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

    def _lint_task(self, task: jobs.Task):
        yield from self._lint_notebook_task(task)
        yield from self._lint_spark_python_task(task)
        yield from self._lint_python_wheel_task(task)
        yield from self._lint_spark_jar_task(task)
        yield from self._lint_libraries(task)
        yield from self._lint_run_job_task(task)
        yield from self._lint_pipeline_task(task)
        yield from self._lint_existing_cluster_id(task)
        yield from self._lint_spark_submit_task(task)

    def _lint_notebook_task(self, task: jobs.Task):
        if not task.notebook_task:
            return
        notebook_path = task.notebook_task.notebook_path
        path = WorkspacePath(self._ws, notebook_path)
        maybe = self._builder.build_notebook_dependency_graph(path)
        for problem in maybe.problems:
            yield problem.source_path, problem.as_advisory()
        if not maybe.graph:
            return
        session_state = CurrentSessionState()
        state = Languages(self._migration_index, session_state)
        for dependency in maybe.graph.all_dependencies:
            container = dependency.load(maybe.graph.path_lookup)
            if not container:
                continue
            if isinstance(container, Notebook):
                yield from self._lint_notebook(container, state)
            if isinstance(container, SitePackageContainer):
                yield from self._lint_python_package(container)
            if isinstance(container, LocalFile):
                yield from self._lint_file(container, state)

    def _lint_python_package(self, site_package):
        for path in site_package.paths:
            # lint every path
            yield path, Advice('not-yet-implemented', 'Python package is not yet implemented', 0, 0, 0, 0)

    def _lint_file(self, file: LocalFile, state: Languages):
        linter = FileLinter(state, file.path)
        for advice in linter.lint():
            yield file.path, advice

    def _lint_notebook(self, notebook: Notebook, state: Languages):
        linter = NotebookLinter(state, notebook)
        for advice in linter.lint():
            yield notebook.path, advice

    _UNKNOWN = Path('<MISSING>')

    def _lint_spark_submit_task(self, task: jobs.Task):
        if not task.spark_submit_task:
            return
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Spark submit task is not yet implemented', 0, 0, 0, 0)

    def _lint_existing_cluster_id(self, task: jobs.Task):
        if not task.existing_cluster_id:
            return
        # TODO: load pypi and dbfs libraries
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Existing cluster ID is not yet implemented', 0, 0, 0, 0)

    def _lint_pipeline_task(self, task: jobs.Task):
        if not task.pipeline_task:
            return
        # TODO: ... load DLT
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Pipeline task is not yet implemented', 0, 0, 0, 0)

    def _lint_run_job_task(self, task: jobs.Task):
        if not task.run_job_task:
            return
        # TODO: something like self.lint(task.run_job_task.job_id)
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Run job task is not yet implemented', 0, 0, 0, 0)

    def _lint_spark_python_task(self, task: jobs.Task):
        if not task.spark_python_task:
            return
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Spark Python task is not yet implemented', 0, 0, 0, 0)

    def _lint_python_wheel_task(self, task: jobs.Task):
        if not task.python_wheel_task:
            return
        # TODO: ... load
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Python wheel task is not yet implemented', 0, 0, 0, 0)

    def _lint_spark_jar_task(self, task: jobs.Task):
        if not task.spark_jar_task:
            return
        # TODO: ... load
        yield self._UNKNOWN, Advice('not-yet-implemented', 'Spark Jar task is not yet implemented', 0, 0, 0, 0)

    def _lint_libraries(self, task: jobs.Task):
        if not task.libraries:
            return
        if task.libraries:
            for library in task.libraries:
                yield from self._lint_library(library)

    def _lint_library(self, library: compute.Library):
        if library.pypi:
            yield self._UNKNOWN, Advice('not-yet-implemented', 'Pypi library is not yet implemented', 0, 0, 0, 0)
        if library.jar:
            yield self._UNKNOWN, Advice('not-yet-implemented', 'Jar library is not yet implemented', 0, 0, 0, 0)
        if library.egg:
            # TODO: load and lint
            yield self._UNKNOWN, Advice('not-yet-implemented', 'Egg library is not yet implemented', 0, 0, 0, 0)
        if library.whl:
            # TODO: load from DBFS
            # TODO: load and lint
            yield self._UNKNOWN, Advice('not-yet-implemented', 'Whl library is not yet implemented', 0, 0, 0, 0)
        if library.requirements:
            # TODO: load and lint
            yield self._UNKNOWN, Advice(
                'not-yet-implemented', 'Requirements library is not yet implemented', 0, 0, 0, 0
            )
