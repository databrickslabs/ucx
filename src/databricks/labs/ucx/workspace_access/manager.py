import logging
from collections.abc import Callable, Iterator
from itertools import groupby
from typing import Literal

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import ThreadedExecution
from databricks.labs.ucx.workspace_access.base import Applier, Crawler, Permissions
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = logging.getLogger(__name__)


class PermissionManager(CrawlerBase):
    def __init__(
        self, backend: SqlBackend, inventory_database: str, crawlers: list[Crawler], appliers: dict[str, Applier]
    ):
        super().__init__(backend, "hive_metastore", inventory_database, "permissions", Permissions)
        self._crawlers = crawlers
        self._appliers = appliers

    def inventorize_permissions(self):
        logger.debug("Crawling permissions")
        crawler_tasks = list(self._get_crawler_tasks())
        logger.info(f"Starting to crawl permissions. Total tasks: {len(crawler_tasks)}")
        results = ThreadedExecution.gather("crawl permissions", crawler_tasks)
        items = []
        for item in results:
            if item is None:
                continue
            if item.object_type not in self._appliers:
                msg = f"unknown object_type: {item.object_type}"
                raise KeyError(msg)
            items.append(item)
        logger.info(f"Total crawled permissions after filtering: {len(items)}")
        self._save(items)
        logger.info(f"Saved {len(items)} to {self._full_name}")

    def apply_group_permissions(self, migration_state: GroupMigrationState, destination: Literal["backup", "account"]):
        # list shall be sorted prior to using group by
        if len(migration_state.groups) == 0:
            logger.info("No valid groups selected, nothing to do.")
            return
        items = sorted(self._load_all(), key=lambda i: i.object_type)
        logger.info(
            f"Applying the permissions to {destination} groups. "
            f"Total groups to apply permissions: {len(migration_state.groups)}. "
            f"Total permissions found: {len(items)}"
        )
        applier_tasks = []
        supports_to_items = {
            support: list(items_subset) for support, items_subset in groupby(items, key=lambda i: i.object_type)
        }

        # we first check that all supports are valid.
        for object_type in supports_to_items:
            if object_type not in self._appliers:
                msg = f"Could not find support for {object_type}. Please check the inventory table."
                raise ValueError(msg)

        for object_type, items_subset in supports_to_items.items():
            relevant_support = self._appliers[object_type]
            tasks_for_support = [
                relevant_support.get_apply_task(item, migration_state, destination) for item in items_subset
            ]
            logger.info(f"Total tasks for {object_type}: {len(tasks_for_support)}")
            applier_tasks.extend(tasks_for_support)

        logger.info(f"Starting to apply permissions on {destination} groups. Total tasks: {len(applier_tasks)}")
        ThreadedExecution.gather(f"apply {destination} group permissions", applier_tasks)
        logger.info("Permissions were applied")

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self._full_name}")
        self._exec(f"DROP TABLE IF EXISTS {self._full_name}")
        logger.info("Inventory table cleanup complete")

    def _save(self, items: list[Permissions]):
        self._append_records(items)  # TODO: update instead of append
        logger.info("Successfully saved the items to inventory table")

    def _load_all(self) -> list[Permissions]:
        logger.info(f"Loading inventory table {self._full_name}")
        return [
            Permissions(object_id, object_type, raw)
            for object_id, object_type, raw in self._fetch(f"SELECT object_id, object_type, raw FROM {self._full_name}")
        ]

    def _get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        for support in self._crawlers:
            yield from support.get_crawler_tasks()
