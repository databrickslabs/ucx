import typing
from dataclasses import dataclass
from functools import partial

from databricks.sdk.service.iam import Group

from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import ThreadedExecution


@dataclass
class GroupPair:
    source: Group
    destination: Group


GroupPairs = list[GroupPair]


class GroupManager:
    SYSTEM_GROUPS: typing.ClassVar[list[str]] = ["users", "admins", "account users"]

    def __init__(self):
        self.config = config_provider.config.groups
        self._group_pairs: GroupPairs = GroupPairs()

    def validate_groups(self):
        logger.info("Starting the groups validation")
        if self.config.selected:
            logger.info("Using the provided group listing")
            self._verify_groups()
        else:
            logger.info("No group listing provided, finding eligible groups automatically")
            self.config.selected = self._find_eligible_groups()
        logger.info("Groups validation complete")

    def list_workspace_groups(self):
        logger.info("Listing all groups in the workspace, this may take a while")
        ws_groups = list(provider.ws.groups.list(attributes="displayName,meta", filter=self._display_name_filter))
        logger.info("Workspace group listing complete")
        return ws_groups

    def _find_eligible_groups(self) -> list[str]:
        logger.info("Finding eligible groups automatically")
        listed_groups = self.list_workspace_groups()
        eligible_groups = [g for g in listed_groups if g.meta.resource_type == "WorkspaceGroup"]
        logger.info(f"Found {len(eligible_groups)} eligible groups")
        return [g.display_name for g in eligible_groups]

    def _verify_group_exists_in_ws(self, group_name: str) -> Group:
        logger.info(f"Verifying group {group_name} exists in workspace")
        found_group = self._get_ws_group(group_name, attributes=["id", "displayName", "meta"])
        assert found_group, f"Group {group_name} doesn't exist on the workspace level"
        return found_group

    def _verify_groups(self):
        for group_name in self.config.selected:
            if group_name in self.SYSTEM_GROUPS:
                msg = f"Cannot migrate system group {self.SYSTEM_GROUPS}"
                raise RuntimeError(msg)
            group = self._verify_group_exists_in_ws(group_name)
            self._verify_group_is_workspace_level(group)

    @property
    def _display_name_filter(self):
        return " and ".join([f'displayName ne "{group}"' for group in self.SYSTEM_GROUPS])

    def _get_ws_group(
        self, group_name, attributes: list[str] | None = None, excluded_attributes: list[str] | None = None
    ) -> Group | None:
        filter_string = f'displayName eq "{group_name}" and ' + self._display_name_filter
        groups = list(
            provider.ws.groups.list(
                filter=filter_string,
                attributes=",".join(attributes) if attributes else None,
                excluded_attributes=",".join(excluded_attributes) if excluded_attributes else None,
            )
        )
        if len(groups) == 0:
            return None
        else:
            return groups[0]

    @staticmethod
    def _get_clean_group_info(group: Group, cleanup_keys: list[str] | None = None) -> dict:
        """
        Returns a dictionary with group information, excluding some keys
        :param group: Group object from SDK
        :param cleanup_keys: default (with None) ["id", "externalId", "displayName"]
        :return: dictionary with group information
        """

        cleanup_keys = cleanup_keys or ["id", "externalId", "displayName"]
        group_info = group.as_dict()

        for key in cleanup_keys:
            if key in group_info:
                group_info.pop(key)

        return group_info

    def create_or_update_backup_groups(self):
        logger.info("Creating or updating the backup groups")
        logger.info(f"In total, {len(self.config.selected)} group(s) to be created or updated")

        def _create_or_update_backup_group(group_name: str) -> GroupPair:
            backup_group_name = f"{self.config.backup_group_prefix}{group_name}"
            logger.info(f"Preparing backup group for {group_name} -> {backup_group_name}")
            group = self._get_ws_group(group_name, excluded_attributes=["id", "externalId"])

            group_object = Group.from_dict(self._get_clean_group_info(group))
            group_object.display_name = backup_group_name

            assert group, f"Group {group_name} not found"
            backup_group = self._get_ws_group(backup_group_name, attributes=["id", "displayName", "meta"])

            logger.info(f"Checking if backup group {backup_group_name} already exists")

            if backup_group:
                logger.info(f"Updating backup group {backup_group_name} from the source group {group_name}")
                group_object.id = backup_group.id
                provider.ws.groups.update(id=group_object.id, request=group_object)
                logger.info(f"Backup group {backup_group_name} successfully updated")
            else:
                logger.info("Backup group is not yet created, creating it")
                backup_group = provider.ws.groups.create(request=group_object)
                logger.info(f"Backup group {backup_group_name} successfully created")

            return GroupPair(source=group, destination=backup_group)

        executables = [partial(_create_or_update_backup_group, group_name) for group_name in self.config.selected]
        self._group_pairs = ThreadedExecution[GroupPair](executables).run()

        logger.info("Backup groups were successfully created or updated")

    def _load_group_pairs(self) -> GroupPairs:
        # loading backup groups
        filter_string = f'displayName sw "{self.config.backup_group_prefix}" and ' + self._display_name_filter
        backup_groups = list(
            filter(
                lambda g: g.meta.resource_type == "WorkspaceGroup",
                provider.ws.groups.list(attributes="displayName,id,meta", filter=filter_string),
            )
        )
        source_groups = [
            self._get_ws_group(g.display_name.replace(self.config.backup_group_prefix, "")) for g in backup_groups
        ]

        pairs = [
            GroupPair(source=source, destination=backup)
            for source, backup in zip(source_groups, backup_groups, strict=True)
        ]
        return pairs

    @property
    def group_pairs(self) -> GroupPairs:
        if not self._group_pairs:
            # this is for cases when create_or_update_backup_groups wasn't called
            logger.info("Group pairs are not defined, accessing them from the workspace")
            pairs = self._load_group_pairs()
            self._group_pairs = pairs
            return pairs
        else:
            return self._group_pairs

    @staticmethod
    def _verify_group_is_workspace_level(group: Group):
        error_message = f"Group {group.display_name} is not a workspace level group"
        assert group.meta.resource_type == "WorkspaceGroup", error_message

    def replace_workspace_groups_with_account_groups(self):
        logger.info("Replacing the workspace groups with account-level groups")
        logger.info(f"In total, {len(self.config.selected)} group(s) to be replaced")

        def replace_group(group_name: str):
            ws_group = self._get_ws_group(group_name)

            if ws_group:
                logger.info(f"Deleting the workspace-level group {ws_group.display_name} with id {ws_group.id}")
                provider.ws.groups.delete(ws_group.id)
                logger.info(f"Workspace-level group {ws_group.display_name} with id {ws_group.id} was deleted")
            else:
                logger.warning(f"Workspace-level group {group_name} does not exist, skipping")

            group_info = provider.ws.reflect_account_group_to_workspace(group_name)
            backup_group_name = f"{self.config.backup_group_prefix}{group_name}"
            logger.info(f"Updating group roles and entitlements from backup group {backup_group_name} to {group_name}")
            backup_group = self._get_ws_group(backup_group_name)
            new_group_object = Group.from_dict(
                self._get_clean_group_info(backup_group, cleanup_keys=["id", "externalId", "displayName", "meta"])
            )
            new_group_object.id = group_info.id
            new_group_object.display_name = group_name
            provider.ws.groups.update(id=new_group_object.id, request=new_group_object)

        executables = [partial(replace_group, group_name) for group_name in self.config.selected]
        ThreadedExecution(executables).run()
        logger.info("Workspace groups were successfully replaced with account-level groups")

    def delete_backup_groups(self):
        logger.info("Deleting the workspace-level backup groups")
        logger.info(f"In total, {len(self.config.selected)} group(s) to be deleted")
        for group_name in self.config.selected:
            temp_group_name = f"{self.config.backup_group_prefix}{group_name}"
            logger.info(f"Deleting backup group {temp_group_name}")
            group = self._get_ws_group(temp_group_name, attributes=["id", "displayName"])

            if not group:
                logger.info(f"Group {temp_group_name} does not exist, skipping")
                continue

            provider.ws.groups.delete(id=group.id)

        logger.info("Backup groups were successfully deleted")
