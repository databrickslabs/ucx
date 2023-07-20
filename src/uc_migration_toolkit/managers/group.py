import logging
import typing

from databricks.sdk.service.iam import Group

from uc_migration_toolkit.config import MigrationConfig
from uc_migration_toolkit.providers.client import ClientMixin


class GroupManager(ClientMixin):
    SYSTEM_GROUPS: typing.ClassVar[list[str]] = ["users", "admins", "account-users"]

    def __init__(self, config: MigrationConfig):
        super().__init__(config)
        self.config = config

    def validate_groups(self):
        self.logger.info("Starting the groups validation")
        if self.config.group_listing_config.groups:
            self.logger.info("Using the provided group listing")
            self._verify_groups()
        else:
            self.config.group_listing_config.groups = self._find_eligible_groups()
        self.logger.info("Groups validation complete")

    def list_workspace_groups(self):
        self.logger.info("Listing all groups in workspace, this may take a while")
        ws_groups = list(self.ws_client.groups.list(attributes="displayName", filter=self._display_name_filter))
        self.logger.info("Workspace group listing complete")
        return ws_groups

    async def _find_eligible_groups(self) -> list[str]:
        self.logger.info("Finding eligible groups automatically")
        listed_groups = self.list_workspace_groups()
        eligible_groups = [g for g in listed_groups if g.meta.resource_type == "WorkspaceGroup"]
        self.logger.info(f"Found {len(eligible_groups)} eligible groups")
        return eligible_groups

    def _verify_group_exists_in_ws(self, group_name: str) -> Group:
        self.logger.info(f"Verifying group {group_name} exists in workspace")
        found_group = self._get_ws_group(group_name, attributes=["id", "displayName", "meta"])
        assert found_group, f"Group {group_name} doesn't exist on the workspace level"
        return found_group

    def _verify_groups(self):
        for group_name in self.config.group_listing_config.groups:
            if group_name in self.SYSTEM_GROUPS:
                msg = "Cannot migrate system groups {self.SYSTEM_GROUPS}"
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
            self.ws_client.groups.list(
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

    def create_or_update_temporary_groups(self):
        for group_name in self.config.group_listing_config.groups:
            temp_group_name = f"{self.config.backup_group_prefix}{group_name}"
            logging.info(f"Preparing temporary group for {group_name} -> {temp_group_name}")
            group = self._get_ws_group(group_name, excluded_attributes=["id", "externalId"])

            assert group, f"Group {group_name} not found"
            temp_group = self._get_ws_group(temp_group_name, attributes=["id"])

            if temp_group:
                logging.info(f"Temporary group {temp_group_name} already exists, updating it from original group")
                group.as_dict()
                logging.info(f"Updating temporary group {temp_group_name} from the source group {group_name}")
                self.ws_client.groups.update(temp_group.id, self._get_clean_group_info(group))
            else:
                logging.info("Temporary group is not yet created, creating it")
                self.ws_client.groups.create(temp_group_name, self._get_clean_group_info(group))

    @staticmethod
    def _verify_group_is_workspace_level(group: Group):
        error_message = f"Group {group.display_name} is not a workspace level group"
        assert group.meta.resource_type == "WorkspaceGroup", error_message
