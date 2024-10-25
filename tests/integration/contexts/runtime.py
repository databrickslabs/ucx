import logging
from datetime import timedelta
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.install import deploy_schema
from databricks.labs.ucx.installer.workflows import DeployedWorkflows
from databricks.labs.ucx.runtime import Workflows
from tests.integration.contexts.common import IntegrationContext

logger = logging.getLogger(__name__)


class MockRuntimeContext(IntegrationContext, RuntimeContext):
    def __init__(self, *args):
        super().__init__(*args)
        RuntimeContext.__init__(self)

    @cached_property
    def deployed_workflows(self) -> DeployedWorkflows:
        return InProcessDeployedWorkflows(self)

    @cached_property
    def workspace_installation(self):
        return MockWorkspaceInstallation(self.sql_backend, self.config, self.installation)


class InProcessDeployedWorkflows(DeployedWorkflows):
    """This class runs workflows on the client side instead of deploying them to Databricks."""

    def __init__(self, ctx: RuntimeContext):
        super().__init__(ctx.workspace_client, ctx.install_state)
        self._workflows = {workflow.name: workflow for workflow in Workflows.definitions()}
        self._ctx = ctx

    def run_workflow(self, step: str, skip_job_wait: bool = False, max_wait: timedelta = timedelta(minutes=20)):
        workflow = self._workflows[step]
        incoming = {task.name: 0 for task in workflow.tasks()}
        downstreams = {task.name: [] for task in workflow.tasks()}
        queue: list[str] = []
        for task in workflow.tasks():
            task.workflow = workflow.name
            for dep in task.depends_on:
                downstreams[dep].append(task.name)
                incoming[task.name] += 1
        for task in workflow.tasks():
            if incoming[task.name] == 0:
                queue.append(task.name)
        while queue:
            task_name = queue.pop(0)
            fn = getattr(workflow, task_name)
            # TODO: capture error logs and fail if there is ERROR event, to simulate parse_logs meta-task
            fn(self._ctx)
            for dep_name in downstreams[task_name]:
                incoming[dep_name] -= 1
                if incoming[dep_name] == 0:
                    queue.append(dep_name)

    def relay_logs(self, workflow: str | None = None):
        pass  # noop


class MockWorkspaceInstallation:
    def __init__(self, sql_backend: SqlBackend, config: WorkspaceConfig, installation: Installation):
        self._sql_backend = sql_backend
        self._config = config
        self._installation = installation

    def run(self):
        deploy_schema(self._sql_backend, self._config.inventory_database)

    @property
    def config(self):
        return self._config

    @property
    def folder(self):
        return self._installation.install_folder()

    def uninstall(self):
        pass  # noop
