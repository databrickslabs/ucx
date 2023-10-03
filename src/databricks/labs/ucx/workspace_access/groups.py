import json
import logging
import typing
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.framework.parallel import ThreadedExecution
from databricks.labs.ucx.mixins.hardening import rate_limited

logger = logging.getLogger(__name__)

GroupLevel = typing.Literal["workspace", "account"]


@dataclass
class MigrationGroupInfo:
    workspace: Group
    backup: Group
    account: Group


class GroupMigrationState:
    """Holds migration state of workspace-to-account groups"""

    def __init__(self):
        self.groups: list[MigrationGroupInfo] = []

    def add(self, group: MigrationGroupInfo):
        self.groups.append(group)

    def is_in_scope(self, attr: str, group: Group) -> bool:
        for info in self.groups:
            if getattr(info, attr).id == group.id:
                return True
        return False

    def get_by_workspace_group_name(self, workspace_group_name: str) -> MigrationGroupInfo | None:
        found = [g for g in self.groups if g.workspace.display_name == workspace_group_name]
        if len(found) == 0:
            return None
        else:
            return found[0]


class GroupManager:
    SYSTEM_GROUPS: typing.ClassVar[list[str]] = ["users", "admins", "account users"]
    SCIM_ATTRIBUTES = "id,displayName,meta,members"

    def __init__(self, ws: WorkspaceClient, groups: GroupsConfig):
        self._ws = ws
        self.config = groups
        self._migration_state: GroupMigrationState = GroupMigrationState()
        self._account_groups = self._list_account_groups()
        self._workspace_groups = self._list_workspace_groups()

    def _list_workspace_groups(self) -> list[iam.Group]:
        logger.debug("Listing workspace groups...")
        workspace_groups = [
            g
            for g in self._ws.groups.list(attributes=self.SCIM_ATTRIBUTES)
            if g.meta.resource_type == "WorkspaceGroup" and g.display_name not in self.SYSTEM_GROUPS
        ]
        logger.debug(f"Found {len(workspace_groups)} workspace groups")
        return sorted(workspace_groups, key=lambda _: _.display_name)

    def _list_account_groups(self) -> list[iam.Group]:
        # TODO: we should avoid using this method, as it's not documented
        # get account-level groups even if they're not (yet) assigned to a workspace
        logger.debug("Listing account groups...")
        account_groups = [
            iam.Group.from_dict(r)
            for r in self._ws.api_client.do(
                "get",
                "/api/2.0/account/scim/v2/Groups",
                query={"attributes": self.SCIM_ATTRIBUTES},
            ).get("Resources", [])
        ]
        account_groups = [g for g in account_groups if g.display_name not in self.SYSTEM_GROUPS]
        logger.debug(f"Found {len(account_groups)} account groups")
        return sorted(account_groups, key=lambda _: _.display_name)

    def _get_group(self, group_name, level: GroupLevel) -> iam.Group | None:
        relevant_level_groups = self._workspace_groups if level == "workspace" else self._account_groups
        for group in relevant_level_groups:
            if group.display_name == group_name:
                return group

    def _get_or_create_backup_group(self, source_group_name: str, source_group: iam.Group) -> iam.Group:
        backup_group_name = f"{self.config.backup_group_prefix}{source_group_name}"
        backup_group = self._get_group(backup_group_name, "workspace")

        if backup_group:
            logger.info(f"Backup group {backup_group_name} already exists, no action required")
            return backup_group

        logger.info(f"Creating backup group {backup_group_name}")
        backup_group = self._ws.groups.create(
            display_name=backup_group_name,
            meta=source_group.meta,
            entitlements=source_group.entitlements,
            roles=source_group.roles,
            members=source_group.members,
        )
        self._workspace_groups.append(backup_group)
        logger.info(f"Backup group {backup_group_name} successfully created")

        return backup_group

    def _set_migration_groups(self, groups_names: list[str]):
        def get_group_info(name: str):
            ws_group = self._get_group(name, "workspace")
            assert ws_group, f"Group {name} not found on the workspace level"
            acc_group = self._get_group(name, "account")
            assert acc_group, f"Group {name} not found on the account level"
            backup_group = self._get_or_create_backup_group(source_group_name=name, source_group=ws_group)
            return MigrationGroupInfo(workspace=ws_group, backup=backup_group, account=acc_group)

        collected_groups = ThreadedExecution.gather(
            "get group info", [partial(get_group_info, group_name) for group_name in groups_names]
        )
        for g in collected_groups:
            self._migration_state.add(g)

        logger.info(f"Prepared {len(collected_groups)} groups for migration")

    def _replace_group(self, migration_info: MigrationGroupInfo):
        ws_group = migration_info.workspace

        logger.info(f"Deleting the workspace-level group {ws_group.display_name} with id {ws_group.id}")
        self._ws.groups.delete(ws_group.id)

        # delete ws_group from the list of workspace groups
        self._workspace_groups = [g for g in self._workspace_groups if g.id != ws_group.id]

        logger.info(f"Workspace-level group {ws_group.display_name} with id {ws_group.id} was deleted")

        self._reflect_account_group_to_workspace(migration_info.account)

    @rate_limited(max_requests=5)  # assumption
    def _reflect_account_group_to_workspace(self, acc_group: iam.Group) -> None:
        logger.info(f"Reflecting group {acc_group.display_name} to workspace")

        # TODO: add OpenAPI spec for it
        principal_id = acc_group.id
        permissions = ["USER"]
        path = f"/api/2.0/preview/permissionassignments/principals/{principal_id}"
        self._ws.api_client.do("PUT", path, data=json.dumps({"permissions": permissions}))

        logger.info(f"Group {acc_group.display_name} successfully reflected to workspace")

    # please keep the public methods below this line

    def prepare_groups_in_environment(self):
        logger.info(
            "Preparing groups in the current environment. At this step we'll verify that all groups "
            "exist and are of the correct type. If some temporary groups are missing, they'll be created"
        )
        group_names = self.config.selected
        valid_group_names = []
        if group_names:
            logger.info("Using the provided group listing")

            for g in group_names:
                if g in self.SYSTEM_GROUPS:
                    logger.info(f"Cannot migrate system group {g}. {g} will be skipped.")
                    continue
                if not self._get_group(g, "workspace"):
                    logger.info(f"Group {g} not found on the workspace level. {g} will be skipped.")
                    continue
                if not self._get_group(g, "account"):
                    logger.info(
                        f"Group {g} not found on the account level. {g} will be skipped. You can add {g} "
                        f"to the account and rerun the job."
                    )
                    continue
                valid_group_names.append(g)
        else:
            logger.info(
                "No group listing provided, all available workspace-level groups that have an account-level "
                "group with the same name will be used"
            )
            ws_group_names = {_.display_name for _ in self._workspace_groups}
            ac_group_names = {_.display_name for _ in self._account_groups}
            valid_group_names = list(ws_group_names.intersection(ac_group_names))

        self._set_migration_groups(valid_group_names)
        logger.info("Environment prepared successfully")

    def has_groups(self) -> bool:
        return len(self._migration_state.groups) > 0

    @property
    def migration_groups_provider(self) -> GroupMigrationState:
        if len(self._migration_state.groups) == 0:
            logger.info("No groups were loaded or initialized, nothing to do")
        return self._migration_state

    def replace_workspace_groups_with_account_groups(self):
        logger.info("Replacing the workspace groups with account-level groups")
        if len(self._migration_state.groups) == 0:
            logger.info("No groups were loaded or initialized, nothing to do")
            return
        ThreadedExecution.gather(
            "groups: workspace -> account",
            [partial(self._replace_group, migration_info) for migration_info in self.migration_groups_provider.groups],
        )
        logger.info("Workspace groups were successfully replaced with account-level groups")

    def delete_backup_groups(self):
        if len(self._migration_state.groups) == 0:
            return
        logger.info(
            f"Deleting the workspace-level backup groups. "
            f"In total, {len(self.migration_groups_provider.groups)} group(s) to be deleted"
        )

        for migration_info in self.migration_groups_provider.groups:
            try:
                self._ws.groups.delete(id=migration_info.backup.id)
            except Exception as e:
                logger.warning(
                    f"Failed to delete backup group {migration_info.backup.display_name} "
                    f"with id {migration_info.backup.id}"
                )
                logger.warning(f"Original exception {e}")

        logger.info("Backup groups were successfully deleted")
