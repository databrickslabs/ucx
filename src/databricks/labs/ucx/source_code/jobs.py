from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.source_code.graph import DependencyResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader


class WorkflowLinter:
    def __init__(self, ws: WorkspaceClient, dependency_resolver: DependencyResolver):
        self._ws = ws
        self._dependency_resolver = dependency_resolver

    def lint(self, job_id: int):
        job = self._ws.jobs.get(job_id)
        for task in job.settings.tasks:
            if task.notebook_task:
                self._lint_notebook(job_id)

        notebook = NotebookLoader(self._ws).load(job_id)
        graph = self._dependency_graph_builder.build(notebook)
        return graph.lint()
