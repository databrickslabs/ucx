import os
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.installer.workflows import DeployedWorkflows

INSTALL_FOLDER_ENV = "UCX_INSTALL_FOLDER"


class AppContext(WorkspaceContext):
    """Workspace context for the app.

    The UCX installation is located the same way as the CLI does it (the user's home folder or the global
    ``/Applications/ucx`` folder), unless ``UCX_INSTALL_FOLDER`` pins it explicitly. Pinning is required when the app
    runs as its own service principal, because the service principal has no home folder with an installation.

    Jobs API calls go through ``jobs_client``, which can differ from ``workspace_client``: see
    :mod:`databricks.labs.ucx.app.auth`.
    """

    def __init__(self, ws: WorkspaceClient, jobs_ws: WorkspaceClient | None = None):
        super().__init__(ws)
        self._jobs_ws = jobs_ws or ws

    @cached_property
    def jobs_client(self) -> WorkspaceClient:
        return self._jobs_ws

    @cached_property
    def deployed_workflows(self) -> DeployedWorkflows:
        # The install state (which job is which workflow) is read with the workspace client, the jobs with the jobs
        # client.
        return DeployedWorkflows(self.jobs_client, self.install_state)

    @cached_property
    def installation(self) -> Installation:
        install_folder = os.environ.get(INSTALL_FOLDER_ENV)
        if install_folder:
            return Installation(self.workspace_client, self.product_info.product_name(), install_folder=install_folder)
        return super().installation
