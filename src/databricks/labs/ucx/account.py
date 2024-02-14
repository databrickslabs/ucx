import logging
from typing import ClassVar

import requests
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import ComplexValue, Group, Patch, PatchOp, PatchSchema
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
        return self._ac.get_workspace_client(workspace)

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

    def create_account_level_groups(self, prompts: Prompts):
        acc_groups = self._get_account_groups()
        all_valid_workspace_groups = self._get_valid_workspaces_groups(prompts)

        for group_name, valid_group in all_valid_workspace_groups.items():
            if group_name in acc_groups:
                logger.info(f"Group {group_name} already exist in the account, ignoring")
                continue

            acc_group = self._ac.groups.create(display_name=group_name)

            if not acc_group.id:
                continue
            if len(valid_group.members) > 0:
                self._add_members_to_acc_group(self._ac, acc_group.id, group_name, valid_group)
            logger.info(f"Group {group_name} created in the account")

    def _add_members_to_acc_group(
        self, acc_client: AccountClient, acc_group_id: str, group_name: str, valid_group: Group
    ):
        for chunk in self._chunks(valid_group.members, 20):
            logger.debug(f"Adding {len(chunk)} members to acc group {group_name}")
            acc_client.groups.patch(
                acc_group_id,
                operations=[Patch(op=PatchOp.ADD, path="members", value=[x.as_dict() for x in chunk])],
                schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )

    def _chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def _get_valid_workspaces_groups(self, prompts: Prompts) -> dict[str, Group]:
        all_workspaces_groups: dict[str, Group] = {}

        for workspace in self._workspaces():
            client = self.client_for(workspace)
            logger.info(f"Crawling groups in workspace {client.config.host}")

            ws_group_ids = client.groups.list(attributes="id")
            for group_id in ws_group_ids:
                if not group_id.id:
                    continue

                full_workspace_group = client.groups.get(group_id.id)
                group_name = full_workspace_group.display_name

                if self._is_group_out_of_scope(full_workspace_group):
                    continue

                if group_name in all_workspaces_groups:
                    if self._has_same_members(all_workspaces_groups[group_name], full_workspace_group):
                        logger.info(f"Workspace group {group_name} already found, ignoring")
                        continue

                    if prompts.confirm(
                        f"Group {group_name} does not have the same amount of members "
                        f"in workspace {client.config.host} than previous workspaces which contains the same group name,"
                        f"it will be created at the account with name : {workspace.workspace_name}_{group_name}"
                    ):
                        all_workspaces_groups[f"{workspace.workspace_name}_{group_name}"] = full_workspace_group
                        continue

                if not group_name:
                    continue

                logger.info(f"Found new group {group_name}")
                all_workspaces_groups[group_name] = full_workspace_group

            logger.info(f"Found a total of {len(all_workspaces_groups)} groups to migrate to the account")

        return all_workspaces_groups

    def _is_group_out_of_scope(self, group: Group) -> bool:
        if group.display_name in ["users", "admins", "account users"]:
            logger.debug(f"Group {group.display_name} is a system group, ignoring")
            return True
        meta = group.meta
        if not meta:
            return False
        if meta.resource_type != "WorkspaceGroup":
            logger.debug(f"Group {group.display_name} is an account group, ignoring")
            return True
        return False

    def _has_same_members(self, group_1: Group, group_2: Group) -> bool:
        ws_members_set_1 = set([m.display for m in group_1.members] if group_1.members else [])
        ws_members_set_2 = set([m.display for m in group_2.members] if group_2.members else [])
        return not bool((ws_members_set_1 - ws_members_set_2).union(ws_members_set_2 - ws_members_set_1))

    def _get_account_groups(self) -> dict[str | None, list[ComplexValue] | None]:
        logger.debug("Listing groups in account")
        acc_groups = {}
        for acc_grp_id in self._ac.groups.list(attributes="id"):
            if not acc_grp_id.id:
                continue
            full_account_group = self._ac.groups.get(acc_grp_id.id)
            logger.debug(f"Found account group {acc_grp_id.display_name}")
            acc_groups[full_account_group.display_name] = full_account_group.members

        logger.info(f"{len(acc_groups)} account groups found")
        return acc_groups


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
