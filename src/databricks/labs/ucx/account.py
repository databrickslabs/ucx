import json
import logging
from typing import ClassVar

import requests
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import ComplexValue, Group
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

    SYNC_FILE_NAME: ClassVar[str] = "workspaces.json"

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
        for workspace in self._configured_workspaces():
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
        for workspace in self._configured_workspaces():
            workspaces.append(workspace.as_dict())
        info = json.dumps(workspaces, indent=2).encode("utf8")
        for ws in self.workspace_clients():
            installation_manager = self._new_installation_manager(ws)
            for installation in installation_manager.user_installations():
                path = f"{installation.path}/{self.SYNC_FILE_NAME}"
                ws.workspace.upload(path, info, overwrite=True, format=ImportFormat.AUTO)

    def create_account_level_groups(self):
        """
        Crawl all workspaces, and create account level groups if a WS local group is not present in the account.
        The feature is not configurable, meaning that it fetches all workspaces groups and all account groups.
        """
        acc_groups = self._get_account_groups()

        all_valid_workspace_groups = self._get_valid_workspaces_groups()

        for group_name, group in all_valid_workspace_groups.items():
            if group_name in acc_groups:
                logger.info(f"Group {group_name} already exist in the account, ignoring")
            else:
                self._ac.groups.create(display_name=group_name, members=group.members)
                logger.info(f"Group {group_name} created in the account")

    def _get_valid_workspaces_groups(self) -> dict[str, Group]:
        all_workspaces_groups: dict[str, Group] = {}
        for client in self.workspace_clients():
            ws_group_ids = client.groups.list(attributes="id")
            for group_id in ws_group_ids:
                full_workspace_group = client.groups.get(group_id.id)
                group_name = full_workspace_group.display_name

                if group_name in all_workspaces_groups:
                    if self._has_not_same_members(all_workspaces_groups[group_name], full_workspace_group):
                        logger.warning(
                            f"Group {group_name} does not have same amount of members "
                            f"in workspace {client.config.host}, it will be created with account "
                            f"name {client.config.host}_{group_name}"
                        )
                        all_workspaces_groups[f"{client.config.host}_{group_name}"] = full_workspace_group
                    else:
                        logger.info(f"Workspace group {group_name} already found, ignoring")
                else:
                    logger.info(f"Found new group {group_name}")
                    all_workspaces_groups[group_name] = full_workspace_group
        return all_workspaces_groups

    def _has_not_same_members(self, group_1: Group, group_2: Group) -> bool:
        ws_members_set = set([m.display for m in group_1.members] if group_1.members else [])
        ws_members_set_2 = set([m.display for m in group_2.members] if group_2.members else [])
        return bool((ws_members_set - ws_members_set_2).union(ws_members_set_2 - ws_members_set))

    def _get_account_groups(self) -> dict[str | None, list[ComplexValue] | None]:
        acc_groups = {}
        for acc_grp_id in self._ac.groups.list(attributes="id"):
            full_account_group = self._ac.groups.get(acc_grp_id.id)
            acc_groups[full_account_group.display_name] = full_account_group.members
        return acc_groups


class WorkspaceInfo:
    def __init__(self, ws: WorkspaceClient, folder: str | None = None, new_installation_manager=InstallationManager):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._ws = ws
        self._folder = folder
        self._new_installation_manager = new_installation_manager

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
            with self._ws.workspace.download(f"{self._folder}/{AccountWorkspaces.SYNC_FILE_NAME}") as f:
                for workspace_metadata in json.loads(f.read()):
                    workspace = Workspace.from_dict(workspace_metadata)
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
            workspace = Workspace(workspace_id=int(workspace_id), workspace_name=workspace_name)
            workspaces.append(workspace.as_dict())
            answer = prompts.question("Next workspace id", valid_number=True, default="stop")
            if answer == "stop":
                break
            workspace_id = int(answer)
        info = json.dumps(workspaces, indent=2).encode("utf8")
        installation_manager = self._new_installation_manager(self._ws)
        logger.info("Detecting UCX installations on current workspace...")
        for installation in installation_manager.user_installations():
            path = f"{installation.path}/{AccountWorkspaces.SYNC_FILE_NAME}"
            logger.info(f"Overwriting {path}")
            self._ws.workspace.upload(path, info, overwrite=True, format=ImportFormat.AUTO)  # type: ignore[arg-type]
        logger.info("Synchronised workspace id mapping for installations on current workspace")
