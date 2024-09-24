import logging
from typing import ClassVar

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient, Workspace, AccountClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceConflict
from databricks.sdk.service.iam import ComplexValue, Group, Patch, PatchOp, PatchSchema

logger = logging.getLogger(__name__)


class AccountWorkspaces:
    SYNC_FILE_NAME: ClassVar[str] = "workspaces.json"

    def __init__(self, account_client: AccountClient, include_workspace_ids: list[int] | None = None):
        self._ac = account_client
        self._include_workspace_ids = include_workspace_ids if include_workspace_ids else []

    def _workspaces(self):
        for workspace in self._ac.workspaces.list():
            if self._include_workspace_ids and workspace.workspace_id not in self._include_workspace_ids:
                logger.debug(f"Skipping {workspace.workspace_name} ({workspace.workspace_id}): not in include list")
                continue
            yield workspace

    def client_for(self, workspace: Workspace) -> WorkspaceClient:
        try:
            # get_workspace_client will raise an exception since it calls config.init_auth()
            return self._ac.get_workspace_client(workspace)
        except (PermissionDenied, NotFound, ValueError) as err:
            # on azure, we can retry with azure-cli auth
            if self._ac.config.is_azure and self._ac.config.auth_type != "azure-cli":
                current_auth_type = self._ac.config.auth_type
                self._ac.config.auth_type = "azure-cli"
                try:
                    ws = self._ac.get_workspace_client(workspace)
                except (PermissionDenied, NotFound, ValueError) as exc:
                    raise PermissionDenied(f"Failed to create client for {workspace.deployment_name}: {exc}") from exc
                finally:
                    self._ac.config.auth_type = current_auth_type
                return ws
            raise PermissionDenied(f"Failed to create client for {workspace.deployment_name}: {err}") from err

    def workspace_clients(self, workspaces: list[Workspace] | None = None) -> list[WorkspaceClient]:
        """
        Return a list of WorkspaceClient for each configured workspace in the account
        :return: list[WorkspaceClient]
        """
        if workspaces is None:
            workspaces = self.get_accessible_workspaces()
        clients = []
        for workspace in workspaces:
            ws = self.client_for(workspace)
            clients.append(ws)
        return clients

    def sync_workspace_info(self, workspaces: list[Workspace] | None = None):
        """
        Create a json dump for each Workspace in account
        For each user that has ucx installed in their workspace,
        upload the json dump of workspace info in the .ucx folder
        """
        if workspaces is None:
            workspaces = []
            for workspace in self._workspaces():
                workspaces.append(workspace)
        for ws in self.workspace_clients(workspaces):
            try:
                for installation in Installation.existing(ws, "ucx"):
                    installation.save(workspaces, filename=self.SYNC_FILE_NAME)
            except (PermissionDenied, NotFound, ValueError):
                logger.warning(f"Failed to save workspace info for {ws.config.host}")

    def create_account_level_groups(self, prompts: Prompts, workspace_ids: list[int] | None = None):
        acc_groups = self._get_account_groups()
        workspace_ids = self._get_valid_workspaces_ids(workspace_ids)
        all_valid_workspace_groups = self._get_valid_workspaces_groups(prompts, workspace_ids)

        for group_name, valid_group in all_valid_workspace_groups.items():
            acc_group = self._try_create_account_groups(group_name, acc_groups)

            if not acc_group or not valid_group.members or not acc_group.id:
                continue
            if len(valid_group.members) > 0:
                self._add_members_to_acc_group(self._ac, acc_group.id, group_name, valid_group)
            logger.info(f"Group {group_name} created in the account")

    def get_accessible_workspaces(self) -> list[Workspace]:
        """
        Get all workspaces that the user has access to
        :return: list[Workspace]
        """
        accessible_workspaces = []
        for workspace in self._ac.workspaces.list():
            if self.can_administer(workspace):
                accessible_workspaces.append(workspace)
        return accessible_workspaces

    def can_administer(self, workspace: Workspace) -> bool:
        """Evaluate if the user can administer a workspace.

        A user can administer a workspace if the user can access the workspace and is a member of the workspace "admins"
        group.

        Args:
            workspace (Workspace): The workspace to check if the user can administer.

        Returns:
            bool: True if the user can administer the workspace, False otherwise.
        """
        try:
            ws = self.client_for(workspace)
            current_user = ws.current_user.me()
        except (PermissionDenied, NotFound, ValueError) as e:
            logger.warning(f"User cannot access workspace: {workspace.deployment_name}", exc_info=e)
            return False
        if current_user.groups is None or "admins" not in {g.display for g in current_user.groups}:
            logger.warning(f"User '{current_user.user_name}' is not a workspace admin: {workspace.deployment_name}")
            return False
        return True

    def _try_create_account_groups(
        self, group_name: str, acc_groups: dict[str | None, list[ComplexValue] | None]
    ) -> Group | None:
        try:
            if group_name in acc_groups:
                logger.info(f"Group {group_name} already exist in the account, ignoring")
                return None
            return self._ac.groups.create(display_name=group_name)
        except ResourceConflict:
            logger.info(f"Group {group_name} already exist in the account, ignoring")
            return None

    def _get_valid_workspaces_ids(self, workspace_ids: list[int] | None = None) -> list[int]:
        all_workspace_ids = [workspace.workspace_id for workspace in self._workspaces()]
        if not workspace_ids:
            return all_workspace_ids
        # TODO: remove this method and rely on _include_workspace_ids

        valid_workspace_ids = []
        for workspace_id in workspace_ids:
            if workspace_id in all_workspace_ids:
                valid_workspace_ids.append(workspace_id)
            else:
                logger.info(f"Workspace id {workspace_id} not found on the account")

        if not valid_workspace_ids:
            raise ValueError("No workspace ids provided in the configuration found in the account")

        workspace_ids_str = ','.join(str(x) for x in valid_workspace_ids)
        logger.info(f"Creating account groups for workspaces IDs : {workspace_ids_str}")
        return valid_workspace_ids

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

    @staticmethod
    def _chunks(lst, chunk_size):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

    def _get_valid_workspaces_groups(self, prompts: Prompts, workspace_ids: list[int]) -> dict[str, Group]:
        all_workspaces_groups: dict[str, Group] = {}

        for workspace in self._workspaces():
            if workspace.workspace_id not in workspace_ids:
                continue
            self._load_workspace_groups(prompts, workspace, all_workspaces_groups)

        return all_workspaces_groups

    def _load_workspace_groups(self, prompts, workspace, all_workspaces_groups):
        client = self.client_for(workspace)
        logger.info(f"Crawling groups in workspace {client.config.host}")
        ws_group_ids = client.groups.list(attributes="id")
        for group_id in ws_group_ids:
            full_workspace_group = self._safe_groups_get(client, group_id.id)
            if not full_workspace_group:
                continue
            group_name = full_workspace_group.display_name
            if self._is_group_out_of_scope(full_workspace_group):
                continue
            if not group_name:
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
            logger.info(f"Found new group {group_name}")
            all_workspaces_groups[group_name] = full_workspace_group
        logger.info(f"Found a total of {len(all_workspaces_groups)} groups to migrate to the account")

    @staticmethod
    def _is_group_out_of_scope(group: Group) -> bool:
        if group.display_name in {"users", "admins", "account users"}:
            logger.debug(f"Group {group.display_name} is a system group, ignoring")
            return True
        meta = group.meta
        if not meta:
            return False
        if meta.resource_type != "WorkspaceGroup":
            logger.debug(f"Group {group.display_name} is an account group, ignoring")
            return True
        return False

    @staticmethod
    def _has_same_members(group_1: Group, group_2: Group) -> bool:
        ws_members_set_1 = set([m.display for m in group_1.members] if group_1.members else [])
        ws_members_set_2 = set([m.display for m in group_2.members] if group_2.members else [])
        return not bool((ws_members_set_1 - ws_members_set_2).union(ws_members_set_2 - ws_members_set_1))

    def _get_account_groups(self) -> dict[str | None, list[ComplexValue] | None]:
        logger.debug("Listing groups in account")
        acc_groups = {}
        for acc_grp_id in self._ac.groups.list(attributes="id"):
            if not acc_grp_id.id:
                continue
            full_account_group = self._safe_groups_get(self._ac, acc_grp_id.id)
            if not full_account_group:
                continue
            logger.debug(f"Found account group {full_account_group.display_name}")
            acc_groups[full_account_group.display_name] = full_account_group.members

        logger.info(f"{len(acc_groups)} account groups found")
        return acc_groups

    @staticmethod
    def _safe_groups_get(interface, group_id) -> Group | None:
        try:
            if not group_id:
                return None
            return interface.groups.get(group_id)
        except NotFound:
            logger.info(f"Group {group_id} has been deleted")
            return None


class WorkspaceInfo:
    def __init__(self, installation: Installation, ws: WorkspaceClient):
        self._installation = installation
        self._ws = ws

    def load_workspace_info(self) -> dict[int, Workspace]:
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
        workspace_id = self._ws.get_workspace_id()
        workspaces = self.load_workspace_info()
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
        workspace_id = self._ws.get_workspace_id()
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
