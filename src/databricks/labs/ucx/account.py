import json
import logging
from typing import ClassVar

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import AccountConfig
from databricks.labs.ucx.installer import InstallationManager

logger = logging.getLogger(__name__)


class AccountWorkspaces:
    _tlds: ClassVar[dict[str, str]] = {
        "aws": "cloud.databricks.com",
        "azure": "azuredatabricks.net",
        "gcp": "gcp.databricks.com",
    }

    def __init__(
        self, cfg: AccountConfig, new_workspace_client=WorkspaceClient, new_installation_manager=InstallationManager
    ):
        self._new_workspace_client = new_workspace_client
        self._new_installation_manager = new_installation_manager
        self._ac = cfg.to_account_client()
        self._cfg = cfg

    def _configured_workspaces(self):
        for workspace in self._ac.workspaces.list():
            if self._cfg.include_workspace_names:
                if workspace.workspace_name not in self._cfg.include_workspace_names:
                    logger.debug(
                        f"skipping {workspace.workspace_name} ({workspace.workspace_id} because "
                        f"its not explicitly included"
                    )
                    continue
            yield workspace

    def _get_cloud(self) -> str:
        if self._ac.config.is_azure:
            return "azure"
        elif self._ac.config.is_gcp:
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
        for workspace in self._configured_workspaces():
            ws = self.client_for(workspace)
            clients.append(ws)
        return clients

    def sync_workspace_info(self):
        """
        Create a json dump for each Workspace in account
        For each user that has ucx installed in their workspace,
        upload the json dump of workspace info in the .ucx folder
        :return:
        """
        workspaces = []
        for workspace in self._configured_workspaces():
            workspaces.append(workspace.as_dict())
        info = json.dumps(workspaces, indent=2).encode("utf8")
        for ws in self.workspace_clients():
            installation_manager = self._new_installation_manager(ws)
            for installation in installation_manager.user_installations():
                path = f"{installation.path}/workspaces.json"
                ws.workspace.upload(path, info, overwrite=True, format=ImportFormat.AUTO)


class WorkspaceInfoReader:
    def __init__(self, ws: WorkspaceClient, folder: str):
        self._ws = ws
        self._folder = folder

    def _current_workspace_id(self) -> int:
        headers = self._ws.config.authenticate()
        headers["User-Agent"] = self._ws.config.user_agent
        response = requests.get(f"{self._ws.config.host}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=10)
        return int(response.headers.get("x-databricks-org-id"))

    def _load_workspace_info(self) -> dict[int, Workspace]:
        try:
            id_to_workspace = {}
            workspace_info = self._ws.workspace.download(f"{self._folder}/workspace-info.json")
            for workspace_metadata in json.loads(workspace_info):
                workspace = Workspace.from_dict(workspace_metadata)
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
        return workspaces[workspace_id].workspace_name
