import logging
from typing import ClassVar

import requests
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.ucx.__about__ import __version__

logger = logging.getLogger(__name__)


class AccountWorkspaces:
    _tlds: ClassVar[dict[str, str]] = {
        "aws": "cloud.databricks.com",
        "azure": "azuredatabricks.net",
        "gcp": "gcp.databricks.com",
    }

    SYNC_FILE_NAME: ClassVar[str] = "workspaces.json"

    def __init__(self, ac: AccountClient, new_workspace_client=WorkspaceClient):
        self._new_workspace_client = new_workspace_client
        self._ac = ac

    def _workspaces(self):
        return self._ac.workspaces.list()

    def _get_cloud(self) -> str:
        if self._ac.config.is_azure:
            return "azure"
        if self._ac.config.is_gcp:
            return "gcp"
        return "aws"

    def client_for(self, workspace: Workspace) -> WorkspaceClient:
        config = self._ac.config.as_dict()
        if "databricks_cli_path" in config:
            del config["databricks_cli_path"]
        cloud = self._get_cloud()
        # copy current config and swap with a host relevant to a workspace
        config["host"] = f"https://{workspace.deployment_name}.{self._tlds[cloud]}"
        return self._new_workspace_client(**config, product="ucx", product_version=__version__)

    def workspace_clients(self) -> list[WorkspaceClient]:
        """
        Return a list of WorkspaceClient for each configured workspace in the account
        :return: list[WorkspaceClient]
        """
        clients = []
        for workspace in self._workspaces():
            ws = self.client_for(workspace)
            clients.append(ws)
        return clients

    def sync_workspace_info(self):
        """
        Create a json dump for each Workspace in account
        For each user that has ucx installed in their workspace,
        upload the json dump of workspace info in the .ucx folder
        """
        workspaces = []
        for workspace in self._workspaces():
            workspaces.append(workspace)
        for ws in self.workspace_clients():
            for installation in Installation.existing(ws, "ucx"):
                installation.save(workspaces, filename=self.SYNC_FILE_NAME)


class WorkspaceInfo:
    def __init__(self, installation: Installation, ws: WorkspaceClient):
        self._installation = installation
        self._ws = ws

    def _current_workspace_id(self) -> int:
        headers = self._ws.config.authenticate()
        headers["User-Agent"] = self._ws.config.user_agent
        response = requests.get(f"{self._ws.config.host}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=10)
        org_id_header = response.headers.get("x-databricks-org-id", None)
        if not org_id_header:
            msg = "Cannot determine current workspace id"
            raise ValueError(msg)
        return int(org_id_header)

    def _load_workspace_info(self) -> dict[int, Workspace]:
        try:
            id_to_workspace = {}
            for workspace in self._installation.load(list[Workspace], filename=AccountWorkspaces.SYNC_FILE_NAME):
                assert workspace.workspace_id is not None
                id_to_workspace[workspace.workspace_id] = workspace
            return id_to_workspace
        except NotFound:
            msg = "Please run as account-admin: databricks labs ucx sync-workspace-info"
            raise ValueError(msg) from None

    def current(self) -> str:
        workspace_id = self._current_workspace_id()
        workspaces = self._load_workspace_info()
        if workspace_id not in workspaces:
            msg = f"Current workspace is not known: {workspace_id}"
            raise KeyError(msg) from None
        workspace = workspaces[workspace_id]
        assert workspace.workspace_name, "workspace name undefined"
        return workspace.workspace_name

    def manual_workspace_info(self, prompts: Prompts):
        logger.warning(
            'You are strongly recommended to run "databricks labs ucx sync-workspace-info" by account admin,'
            ' otherwise there is a significant risk of inconsistencies between different workspaces. This '
            'command will overwrite all UCX installations on this given workspace. Result may be consistent '
            f'only within {self._ws.config.host}'
        )
        workspaces = []
        workspace_id = self._current_workspace_id()
        while workspace_id:
            workspace_name = prompts.question(
                f"Workspace name for {workspace_id}", default=f"workspace-{workspace_id}", valid_regex=r"^[\w-]+$"
            )
            workspaces.append(Workspace(workspace_id=int(workspace_id), workspace_name=workspace_name))
            answer = prompts.question("Next workspace id", valid_number=True, default="stop")
            if answer == "stop":
                break
            workspace_id = int(answer)
        for installation in Installation.existing(self._ws, 'ucx'):
            installation.save(workspaces, filename=AccountWorkspaces.SYNC_FILE_NAME)
        logger.info("Synchronised workspace id mapping for installations on current workspace")
