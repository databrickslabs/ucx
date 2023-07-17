import logging
from typing import List, Optional

from databricks.sdk.service.iam import Group

from uc_upgrade.config import MigrationConfig
from uc_upgrade.providers.client import ClientProvider
from uc_upgrade.providers.logger import LoggerProvider


class GroupManager:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.logger = LoggerProvider.get_logger()
        self.ws_client = ClientProvider().get_workspace_client(config)
        self.acc_client = ClientProvider().get_account_client(config)

    def _pre_init_groups(self):
        if self.config.group_listing_config.groups:
            self.logger.info("Using the provided group listing")
            self.verify_groups()
        else:
            self.config.group_listing_config.groups = self._find_eligible_groups()

    def list_workspace_groups(self):
        self.logger.info("Listing all groups in workspace, this may take a while")
        ws_groups = list(self.ws_client.groups.list(attributes="displayName"))
        self.logger.info("Workspace group listing complete")
        return ws_groups

    def list_account_groups(self):
        self.logger.info("Listing all groups in account, this may take a while")
        acc_groups = list(self.acc_client.groups.list(attributes="displayName"))
        self.logger.info("Account group listing complete")
        return acc_groups

    async def _find_eligible_groups(self) -> List[str]:
        self.logger.info("Finding eligible groups automatically")
        ws_groups = self.list_workspace_groups()
        acc_groups = self.list_account_groups()
        self.logger.info("Filtering out only groups that exist both in workspace and on the account level")
        eligible_groups = [
            group.display_name for group in ws_groups if group.id in [acc_group.id for acc_group in acc_groups]
        ]
        return eligible_groups

    def _verify_group_exists_in_ws(self, group_name: str):
        self.logger.info(f"Verifying group {group_name} exists in workspace")
        found_group = self._get_ws_group(group_name, attributes="id")
        assert found_group, f"Group {group_name} not found on the workspace level"

    def _verify_group_exists_in_acc(self, group_name: str):
        self.logger.info(f"Verifying group {group_name} exists in account")
        found_group = self._get_account_group(group_name, attributes="id")
        assert found_group, f"Group {group_name} not found"

    def verify_groups(self):
        for group_name in self.config.group_listing_config.groups:
            self._verify_group_exists_in_ws(group_name)
            self._verify_group_exists_in_acc(group_name)

    def _get_ws_group(
            self, group_name, attributes: Optional[str] = None, excluded_attributes: Optional[str] = None
    ) -> Optional[Group]:
        groups = list(
            self.ws_client.groups.list(
                filter=f'displayName eq "{group_name}"', attributes=attributes, excluded_attributes=excluded_attributes
            )
        )
        if len(groups) == 0:
            return None
        else:
            return groups[0]

    def _get_account_group(self, group_name, attributes: Optional[str] = None) -> Optional[Group]:
        groups = list(self.ws_client.groups.list(filter=f'displayName eq "{group_name}"', attributes=attributes))
        if len(groups) == 0:
            return None
        else:
            return groups[0]

    @staticmethod
    def _get_clean_group_info(group: Group, cleanup_keys: Optional[List[str]] = None) -> dict:
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

    def create_or_update_temporary_groups(self, dry_run: bool):
        for group_name in self.config.group_listing_config.groups:
            temp_group_name = f"temp_{group_name}"
            logging.info(f"Preparing temporary group for {group_name} -> {temp_group_name}")
            group = self._get_ws_group(group_name, excluded_attributes="id, externalId")

            assert group, f"Group {group_name} not found"
            temp_group = self._get_ws_group(temp_group_name, attributes="id")

            if temp_group:
                logging.info(f"Temporary group {temp_group_name} already exists, updating it from original group")
                group.as_dict()
                logging.info(f"Updating temporary group {temp_group_name} from the source group {group_name}")
                if not dry_run:
                    self.ws_client.groups.update(temp_group.id, self._get_clean_group_info(group))
            else:
                logging.info("Temporary group is not yet created, creating it")
                if not dry_run:
                    self.ws_client.groups.create(temp_group_name, self._get_clean_group_info(group))
