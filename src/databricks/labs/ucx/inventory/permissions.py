import logging
from typing import Literal

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import PermissionsInventoryItem, Supports
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.supports.base import BaseSupport
from databricks.labs.ucx.utils import ThreadedExecution

logger = logging.getLogger(__name__)


class PermissionManager:
    def __init__(self, ws: WorkspaceClient, permissions_inventory: PermissionsInventoryTable):
        self._ws = ws
        self._permissions_inventory = permissions_inventory
        self._supports: dict[Supports, BaseSupport] = {}

    @property
    def supports(self) -> dict[Supports, BaseSupport]:
        return self._supports

    def set_supports(self, supports: dict[Supports, BaseSupport]):
        self._supports = supports

    def inventorize_permissions(self):
        logger.info("Inventorizing the permissions")
        crawler_tasks = []

        for name, support in self._supports.items():
            logger.info(f"Adding crawler tasks for {name}")
            crawler_tasks.extend(support.get_crawler_tasks())
            logger.info(f"Added crawler tasks for {name}")

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
        items = self._permissions_inventory.load_all()
        logger.info(f"Total inventorized items: {len(items)}")
        applier_tasks = []
        for name, _support in self._supports.items():
            logger.info(f"Adding applier tasks for {name}")
            applier_tasks.extend(
                [self._supports.get(item.support).get_apply_task(item, migration_state, destination) for item in items]
            )
            logger.info(f"Added applier tasks for {name}")

        logger.info(f"Total applier tasks: {len(applier_tasks)}")
        logger.info("Starting the permissions application")
        execution = ThreadedExecution(applier_tasks)
        execution.run()
        logger.info("Permissions were applied")
