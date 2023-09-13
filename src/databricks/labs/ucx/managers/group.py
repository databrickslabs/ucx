import json
import logging
import typing
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.generic import StrEnum
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.utils import ThreadedExecution

logger = logging.getLogger(__name__)


class GroupLevel(StrEnum):
    WORKSPACE = "workspace"
    ACCOUNT = "account"


class GroupManager:
    SYSTEM_GROUPS: typing.ClassVar[list[str]] = ["users", "admins", "account users"]

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
            for g in self._ws.groups.list(attributes="id,displayName,meta")
            if g.meta.resource_type == "WorkspaceGroup" and g.display_name not in self.SYSTEM_GROUPS
        ]
        logger.debug(f"Found {len(workspace_groups)} workspace groups")
        return workspace_groups

    def _list_account_groups(self) -> list[iam.Group]:
        # TODO: we should avoid using this method, as it's not documented
        # unfortunately, there's no other way to consistently get the list of account groups
        logger.debug("Listing account groups...")
        account_groups = [
            iam.Group.from_dict(r)
            for r in self._ws.api_client.do(
                "get",
                "/api/2.0/account/scim/v2/Groups",
                query={
                    "attributes": "id,displayName,meta",
                },
            ).get("Resources", [])
        ]
        account_groups = [g for g in account_groups if g.display_name not in self.SYSTEM_GROUPS]
        logger.debug(f"Found {len(account_groups)} account groups")
        return account_groups

    def _get_group(self, group_name, level: GroupLevel) -> iam.Group | None:
        relevant_level_groups = self._workspace_groups if level == GroupLevel.WORKSPACE else self._account_groups
        for group in relevant_level_groups:
            if group.display_name == group_name:
                return group

    def _get_or_create_backup_group(self, source_group_name: str, source_group: iam.Group) -> iam.Group:
        backup_group_name = f"{self.config.backup_group_prefix}{source_group_name}"
        backup_group = self._get_group(backup_group_name, GroupLevel.WORKSPACE)

        if backup_group:
            logger.info(f"Backup group {backup_group_name} already exists, no action required")
        else:
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
            ws_group = self._get_group(name, GroupLevel.WORKSPACE)
            assert ws_group, f"Group {name} not found on the workspace level"
            acc_group = self._get_group(name, GroupLevel.ACCOUNT)
            assert acc_group, f"Group {name} not found on the account level"
            backup_group = self._get_or_create_backup_group(source_group_name=name, source_group=ws_group)
            return MigrationGroupInfo(workspace=ws_group, backup=backup_group, account=acc_group)

        executables = [partial(get_group_info, group_name) for group_name in groups_names]

        collected_groups = ThreadedExecution[MigrationGroupInfo](executables).run()
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

    @sleep_and_retry
    @limits(calls=5, period=1)  # assumption
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
        logger.info("Preparing groups in the current environment")
        logger.info("At this step we'll verify that all groups exist and are of the correct type")
        logger.info("If some temporary groups are missing, they'll be created")
        if self.config.selected:
            logger.info("Using the provided group listing")

            for g in self.config.selected:
                assert g not in self.SYSTEM_GROUPS, f"Cannot migrate system group {g}"
                assert self._get_group(g, GroupLevel.WORKSPACE), f"Group {g} not found on the workspace level"
                assert self._get_group(g, GroupLevel.ACCOUNT), f"Group {g} not found on the account level"

            self._set_migration_groups(self.config.selected)
        else:
            logger.info("No group listing provided, all available workspace-level groups will be used")
            available_group_names = [g.display_name for g in self._workspace_groups]
            self._set_migration_groups(groups_names=available_group_names)
        logger.info("Environment prepared successfully")

    @property
    def migration_groups_provider(self) -> GroupMigrationState:
        assert len(self._migration_state.groups) > 0, "Migration groups were not loaded or initialized"
        return self._migration_state

    def replace_workspace_groups_with_account_groups(self):
        logger.info("Replacing the workspace groups with account-level groups")
        logger.info(f"In total, {len(self.migration_groups_provider.groups)} group(s) to be replaced")

        executables = [
            partial(self._replace_group, migration_info) for migration_info in self.migration_groups_provider.groups
        ]
        ThreadedExecution(executables).run()
        logger.info("Workspace groups were successfully replaced with account-level groups")

    def delete_backup_groups(self):
        logger.info("Deleting the workspace-level backup groups")
        logger.info(f"In total, {len(self.migration_groups_provider.groups)} group(s) to be deleted")

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
