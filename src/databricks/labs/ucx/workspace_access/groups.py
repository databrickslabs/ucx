import collections
import json
import logging
import typing
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.mixins.hardening import rate_limited

logger = logging.getLogger(__name__)

GroupLevel = typing.Literal["workspace", "account"]


@dataclass
class MigrationGroupInfo:
    workspace: Group
    backup: Group
    account: Group

    def is_name_match(self, name: str) -> bool:
        if self.workspace is not None:
            return name == self.workspace.display_name
        if self.account is not None:
            return name == self.account.display_name
        if self.backup is not None:
            return name == self.backup.display_name
        return False

    def is_id_match(self, group_id: str) -> bool:
        if self.workspace is not None:
            return group_id == self.workspace.id
        if self.account is not None:
            return group_id == self.account.id
        if self.backup is not None:
            return group_id == self.backup.id
        return False


class GroupMigrationState:
    """Holds migration state of workspace-to-account groups"""

    def __init__(self, backup_group_prefix: str = "db-temp-"):
        self.groups: list[MigrationGroupInfo] = []
        self._backup_group_prefix = backup_group_prefix

    def add(self, ws_group: Group, backup_group: Group, acc_group: Group):
        mgi = MigrationGroupInfo(workspace=ws_group, backup=backup_group, account=acc_group)
        self.groups.append(mgi)

    def is_in_scope(self, name: str) -> bool:
        if name is None:
            return False
        for info in self.groups:
            if info.is_name_match(name):
                return True
        return False

    def is_id_in_scope(self, group_id: str) -> bool:
        if group_id is None:
            return False
        for info in self.groups:
            if info.is_id_match(group_id):
                return True
        return False

    def get_by_workspace_group_name(self, workspace_group_name: str) -> MigrationGroupInfo | None:
        # TODO: this method is deprecated, replace all usages by get_target_principal()
        found = [g for g in self.groups if g.workspace.display_name == workspace_group_name]
        if len(found) == 0:
            return None
        else:
            return found[0]

    def get_target_principal(self, name: str, destination: typing.Literal["backup", "account"]) -> str | None:
        for info in self.groups:
            # TODO: this logic has to be changed, once we crawl all wslocal accgroups into a table
            group_for_match = info.workspace
            if group_for_match is None:
                group_for_match = info.account
            if group_for_match.display_name != name:
                continue
            return getattr(info, destination).display_name
        return None

    def get_target_id(self, group_id: str, destination: typing.Literal["backup", "account"]) -> str | None:
        for info in self.groups:
            # TODO: this logic has to be changed, once we crawl all wslocal accgroups into a table
            group_for_match = info.workspace
            if group_for_match is None:
                group_for_match = info.account
            if group_for_match.id != group_id:
                continue
            return getattr(info, destination).id
        return None

    def __len__(self):
        return len(self.groups)


class GroupManager:
    _SYSTEM_GROUPS: typing.ClassVar[list[str]] = ["users", "admins", "account users"]
    _SCIM_ATTRIBUTES = "id,displayName,meta,members"

    def __init__(self, ws: WorkspaceClient, groups: GroupsConfig):
        self._ws = ws
        # TODO: remove groups param in favor of _include_group_names and _backup_group_prefix
        self._include_group_names = groups.selected
        self._backup_group_prefix = groups.backup_group_prefix
        self._migration_state = GroupMigrationState(backup_group_prefix=groups.backup_group_prefix)
        self._account_groups = self._list_account_groups(ws, self._SCIM_ATTRIBUTES)
        self._workspace_groups = self._list_workspace_groups()

    @classmethod
    def prepare_apply_permissions_to_account_groups(cls, ws: WorkspaceClient, backup_group_prefix: str = "db-temp-"):
        scim_attributes = "id,displayName,meta"

        account_groups_by_name = {}
        for g in cls._list_account_groups(ws, scim_attributes):
            account_groups_by_name[g.display_name] = g

        workspace_groups_by_name = {}
        account_groups_in_workspace_by_name = {}
        for g in ws.groups.list(attributes=scim_attributes):
            if g.display_name in cls._SYSTEM_GROUPS:
                continue
            if g.meta.resource_type == "WorkspaceGroup":
                workspace_groups_by_name[g.display_name] = g
            else:
                account_groups_in_workspace_by_name[g.display_name] = g

        migration_state = GroupMigrationState(backup_group_prefix=backup_group_prefix)
        for display_name in account_groups_by_name.keys():
            if display_name not in account_groups_in_workspace_by_name:
                logger.info(f"Skipping account group `{display_name}`: not added to a workspace")
                continue
            backup_group_name = f"{backup_group_prefix}{display_name}"
            if backup_group_name not in workspace_groups_by_name:
                logger.info(f"Skipping account group `{display_name}`: no backup group in workspace")
                continue
            backup_group = workspace_groups_by_name[backup_group_name]
            account_group = account_groups_in_workspace_by_name[display_name]
            migration_state.add(None, backup_group, account_group)

        return migration_state

    def prepare_groups_in_environment(self):
        logger.info(
            "Preparing groups in the current environment. At this step we'll verify that all groups "
            "exist and are of the correct type. If some temporary groups are missing, they'll be created"
        )
        valid_group_names = self._get_valid_group_names_to_migrate()
        self._set_migration_groups_with_backup(valid_group_names)
        logger.info("Environment prepared successfully")

    def has_groups(self) -> bool:
        return len(self._migration_state.groups) > 0

    @property
    def migration_state(self) -> GroupMigrationState:
        if len(self._migration_state) == 0:
            logger.info("No groups were loaded or initialized, nothing to do")
        return self._migration_state

    def replace_workspace_groups_with_account_groups(self):
        logger.info("Replacing the workspace groups with account-level groups")
        if len(self._migration_state) == 0:
            logger.info("No groups were loaded or initialized, nothing to do")
            return
        for migration_info in self.migration_state.groups:
            self._replace_group(migration_info)
        logger.info("Workspace groups were successfully replaced with account-level groups")

    def delete_backup_groups(self):
        backup_groups = self._get_backup_groups()
        if len(backup_groups) == 0:
            logger.info("No backup group found, nothing to do")
            return

        logger.info(f"Deleting {len(backup_groups)} backup workspace-level groups")
        for group in backup_groups:
            self._delete_workspace_group(group)
        logger.info("Backup groups were successfully deleted")

    def get_workspace_membership(self, resource_type: str = "WorkspaceGroup"):
        membership = collections.defaultdict(set)
        for g in self._ws.groups.list(attributes=self._SCIM_ATTRIBUTES):
            if g.display_name in self._SYSTEM_GROUPS:
                continue
            if g.meta.resource_type != resource_type:
                continue
            if g.members is None:
                continue
            for m in g.members:
                membership[g.display_name].add(m.display)
        return membership

    def get_account_membership(self):
        membership = collections.defaultdict(set)
        for g in self._account_groups:
            if g.members is None:
                continue
            for m in g.members:
                membership[g.display_name].add(m.display)
        return membership

    def _list_workspace_groups(self) -> list[iam.Group]:
        logger.info("Listing workspace groups...")
        workspace_groups = [
            g
            for g in self._ws.groups.list(attributes=self._SCIM_ATTRIBUTES)
            if g.meta.resource_type == "WorkspaceGroup" and g.display_name not in self._SYSTEM_GROUPS
        ]
        logger.info(f"Found {len(workspace_groups)} workspace groups")
        return sorted(workspace_groups, key=lambda _: _.display_name)

    @classmethod
    def _list_account_groups(
        cls, ws: WorkspaceClient, attributes: str = "id,displayName,meta,members"
    ) -> list[iam.Group]:
        # TODO: we should avoid using this method, as it's not documented
        # get account-level groups even if they're not (yet) assigned to a workspace
        logger.info("Listing account groups...")
        account_groups = [
            iam.Group.from_dict(r)
            for r in ws.api_client.do(
                "get",
                "/api/2.0/account/scim/v2/Groups",
                query={"attributes": attributes},
            ).get("Resources", [])
        ]
        account_groups = [g for g in account_groups if g.display_name not in cls._SYSTEM_GROUPS]
        logger.info(f"Found {len(account_groups)} account groups")
        return sorted(account_groups, key=lambda _: _.display_name)

    def _get_group(self, group_name, level: GroupLevel) -> iam.Group | None:
        relevant_level_groups = self._workspace_groups if level == "workspace" else self._account_groups
        for group in relevant_level_groups:
            if group.display_name == group_name:
                return group

    @retried(on=[DatabricksError])
    @rate_limited(max_requests=35)
    def _get_or_create_backup_group(self, source_group_name: str, source_group: iam.Group) -> iam.Group:
        backup_group_name = f"{self._backup_group_prefix}{source_group_name}"
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
        )  # TODO: there still could be a corner case, where we get `Group with name db-temp-XXX already exists.`
        self._workspace_groups.append(backup_group)
        logger.info(f"Backup group {backup_group_name} successfully created")

        return backup_group

    def _set_migration_groups_with_backup(self, groups_names: list[str]):
        for name in groups_names:
            ws_group = self._get_group(name, "workspace")
            assert ws_group, f"Group {name} not found on the workspace level"
            acc_group = self._get_group(name, "account")
            assert acc_group, f"Group {name} not found on the account level"
            backup_group = self._get_or_create_backup_group(source_group_name=name, source_group=ws_group)
            self._migration_state.add(ws_group, backup_group, acc_group)
        logger.info(f"Prepared {len(self._migration_state)} groups for migration")

    def _replace_group(self, migration_info: MigrationGroupInfo):
        if migration_info.workspace is not None:
            ws_group = migration_info.workspace
            self._delete_workspace_group(ws_group)
            # delete ws_group from the list of workspace groups
            self._workspace_groups = [g for g in self._workspace_groups if g.id != ws_group.id]
        self._reflect_account_group_to_workspace(migration_info.account)

    @retried(on=[DatabricksError])
    @rate_limited(max_requests=35)
    def _delete_workspace_group(self, ws_group: iam.Group) -> None:
        logger.info(f"Deleting the workspace-level group {ws_group.display_name} with id {ws_group.id}")
        self._ws.groups.delete(id=ws_group.id)
        logger.info(f"Workspace-level group {ws_group.display_name} with id {ws_group.id} was deleted")

    @retried(on=[DatabricksError])
    @rate_limited(max_requests=5)
    def _reflect_account_group_to_workspace(self, acc_group: iam.Group) -> None:
        logger.info(f"Reflecting group {acc_group.display_name} to workspace")

        # TODO: add OpenAPI spec for it
        principal_id = acc_group.id
        permissions = ["USER"]
        path = f"/api/2.0/preview/permissionassignments/principals/{principal_id}"
        self._ws.api_client.do("PUT", path, data=json.dumps({"permissions": permissions}))

        logger.info(f"Group {acc_group.display_name} successfully reflected to workspace")

    def _get_backup_groups(self) -> list[iam.Group]:
        ac_group_names = []
        for g in self._account_groups:
            if self._include_group_names and g.display_name not in self._include_group_names:
                logger.debug(f"skipping {g.display_name} as it is not in {self._include_group_names}")
                continue
            ac_group_names.append(g.display_name)

        backup_groups = []
        for g in self._workspace_groups:
            if not g.display_name.startswith(self._backup_group_prefix):
                continue
            # backup groups are only created for workspace groups that have corresponding account group
            if g.display_name.removeprefix(self._backup_group_prefix) not in ac_group_names:
                continue
            backup_groups.append(g)
        logger.info(f"Found {len(backup_groups)} backup groups")

        return backup_groups

    def _get_valid_group_names_to_migrate(self):
        if self._include_group_names:
            return self._validate_selected_groups(self._include_group_names)
        return self._detect_overlapping_group_names()

    def _detect_overlapping_group_names(self):
        logger.info(
            "No group listing provided, all available workspace-level groups that have an account-level "
            "group with the same name will be used"
        )
        ws_group_names = {_.display_name for _ in self._workspace_groups}
        ac_group_names = {_.display_name for _ in self._account_groups}
        valid_group_names = list(ws_group_names.intersection(ac_group_names))
        logger.info(f"Found {len(valid_group_names)} workspace groups that have corresponding account groups")
        return valid_group_names

    def _validate_selected_groups(self, group_names: list[str]):
        valid_group_names = []
        logger.info("Using the provided group listing")
        for g in group_names:
            if g in self._SYSTEM_GROUPS:
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
        return valid_group_names

    def ws_local_group_deletion_recovery(self):
        account_groups_reflected_on_the_workspace = {
            g.display_name
            for g in self._ws.groups.list(attributes="id,displayName,meta")
            if g.meta.resource_type == "Group" and g.display_name not in self._SYSTEM_GROUPS
        }

        workspace_groups = {_.display_name for _ in self._workspace_groups}
        source_groups = [
            g
            for g in self._workspace_groups
            if g.display_name.removeprefix(self._backup_group_prefix) not in workspace_groups
        ]

        logger.info(
            f"Recovering from workspace-local group deletion. "
            f"In total, {len(source_groups)} temporary groups found, which do not have corresponding workspace groups"
        )

        for backup_group in source_groups:
            source_groups_name = backup_group.display_name.removeprefix(self._backup_group_prefix)
            if source_groups_name in account_groups_reflected_on_the_workspace:
                logger.info(f"Group {source_groups_name} already exists on the workspace level, skipping")
                continue
            ws_local_group = self._ws.groups.create(
                display_name=source_groups_name,
                meta=backup_group.meta,
                entitlements=backup_group.entitlements,
                roles=backup_group.roles,
                members=backup_group.members,
            )
            self._workspace_groups.append(ws_local_group)

            account_groups = [_ for _ in self._account_groups if _.display_name == ws_local_group.display_name]
            if len(account_groups) == 0:
                logger.error(f"Cannot find matching group on account level: {ws_local_group.display_name}")
                continue

            account_group = account_groups[0]
            self._migration_state.add(ws_local_group, backup_group, account_group)
            logger.info(f"Workspace-local group {ws_local_group} successfully recovered")

        logger.info("Workspace-local group recovery completed")
