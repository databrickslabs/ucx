import collections
from pathlib import Path

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.graph import DependencyGraphBuilder
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.sources import Notebook, NotebookLinter
from databricks.labs.ucx.source_code.whitelist import Whitelist


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

    def lint(self, job_id: int):
        job = self._ws.jobs.get(job_id)
        problems: dict[Path, list[Advice]] = collections.defaultdict(list)
        for task in job.settings.tasks:
            if task.notebook_task:
                path = WorkspacePath(self._ws, task.notebook_task.notebook_path)
                maybe = self._builder.build_notebook_dependency_graph(path)
                for problem in maybe.problems:
                    problems[problem.source_path].append(problem.as_advisory())
                for dependency in maybe.graph.all_dependencies:
                    container = dependency.load()
                    if not container:
                        continue
                    if isinstance(container, Notebook):
                        languages = Languages(self._migration_index)
                        linter = NotebookLinter(languages, container)
                        for problem in linter.lint():
                            problems[container.path].append(problem)
            if task.spark_python_task:
                # TODO: ... load from dbfs
                continue
            if task.spark_jar_task:
                # TODO: ...
                continue
            if task.spark_submit_task:
                # TODO: ... --py-files
                continue
            if task.python_wheel_task:
                # TODO: ... load wheel
                continue
            if task.pipeline_task:
                # TODO: load pipeline notebooks
                continue
            if task.run_job_task:
                # TODO: load other job and lint
                continue
            if task.existing_cluster_id:
                # TODO: load pypi and dbfs libraries
                continue
            if task.libraries:
                for library in task.libraries:
                    # TODO: load library
                    continue
                continue
        for job_cluster in job.settings.job_clusters:
            for library in job_cluster.libraries:
                if library.pypi:
                    # TODO: whitelist
                    continue
                if library.dbfs:
                    # TODO: load from DBFS
                    continue
                if library.jar:
                    # TODO: warning
                    continue
                if library.egg:
                    # TODO: load and lint
                    continue
                if library.whl:
                    # TODO: load and lint
                    continue
                if library.requirements:
                    # TODO: load and lint
                    continue
                # TODO: ..
                continue
