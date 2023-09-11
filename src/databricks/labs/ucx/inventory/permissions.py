import logging
from itertools import groupby
from typing import Literal

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.support.impl import SupportsProvider
from databricks.labs.ucx.utils import ThreadedExecution

logger = logging.getLogger(__name__)


class PermissionManager:
    def __init__(
        self, ws: WorkspaceClient, permissions_inventory: PermissionsInventoryTable, supports_provider: SupportsProvider
    ):
        self._ws = ws
        self._permissions_inventory = permissions_inventory
        self._supports_provider = supports_provider

    def inventorize_permissions(self):
        logger.info("Inventorizing the permissions")
        crawler_tasks = list(self._supports_provider.get_crawler_tasks())
        logger.info(f"Total crawler tasks: {len(crawler_tasks)}")
        logger.info("Starting the permissions inventorization")
        execution = ThreadedExecution[PermissionsInventoryItem | None](crawler_tasks)
        results = execution.run()
        items = [item for item in results if item is not None]
        logger.info(f"Total inventorized items: {len(items)}")
        self._permissions_inventory.save(items)
        logger.info("Permissions were inventorized and saved")

    def apply_group_permissions(self, migration_state: GroupMigrationState, destination: Literal["backup", "account"]):
        logger.info(f"Applying the permissions to {destination} groups")
        logger.info(f"Total groups to apply permissions: {len(migration_state.groups)}")
        # list shall be sorted prior to using group by
        items = sorted(self._permissions_inventory.load_all(), key=lambda i: i.support)
        logger.info(f"Total inventorized items: {len(items)}")
        applier_tasks = []
        supports_to_items = {
            support: list(items_subset) for support, items_subset in groupby(items, key=lambda i: i.support)
        }

        # we first check that all supports are valid.
        for support in supports_to_items:
            if support not in self._supports_provider.supports:
                msg = f"Could not find support for {support}. Please check the inventory table."
                raise ValueError(msg)

        for support, items_subset in supports_to_items.items():
            relevant_support = self._supports_provider.supports.get(support)
            if not relevant_support:
                msg = f"Could not find support for {support}. Total items for this support: {len(list(items_subset))}"
                raise ValueError(msg)

            tasks_for_support = [
                relevant_support.get_apply_task(item, migration_state, destination) for item in items_subset
            ]
            logger.info(f"Total tasks for {support}: {len(tasks_for_support)}")
            applier_tasks.extend(tasks_for_support)

        logger.info(f"Total applier tasks: {len(applier_tasks)}")
        logger.info("Starting the permissions application")
        execution = ThreadedExecution(applier_tasks)
        execution.run()
        logger.info("Permissions were applied")
