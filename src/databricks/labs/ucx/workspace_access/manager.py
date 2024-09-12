import logging
from collections.abc import Callable, Iterable, Iterator
from itertools import groupby

from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


class PermissionManager(CrawlerBase[Permissions]):
    """Crawler that captures permissions, intended for configuration-related (non-data) objects.

    The set of objects types captured depends on the (sub)-crawlers supplied to the initializer, but is intended to
    cover configuration-related (non-data) objects such as workspace configuration, dashboards, secrets, SCIM
    entitlements, etc.
    """

    ERRORS_TO_IGNORE = ["FEATURE_DISABLED"]

    def __init__(self, backend: SqlBackend, inventory_database: str, crawlers: list[AclSupport]):
        super().__init__(backend, "hive_metastore", inventory_database, "permissions", Permissions)
        self._acl_support = crawlers

    def _crawl(self) -> Iterable[Permissions]:
        logger.debug("Crawling permissions")
        crawler_tasks = list(self._get_crawler_tasks())
        logger.info(f"Starting to crawl permissions. Total tasks: {len(crawler_tasks)}")
        items, errors = Threads.gather("crawl permissions", crawler_tasks)
        acute_errors = []
        for error in errors:
            if hasattr(error, 'error_code') and error.error_code in self.ERRORS_TO_IGNORE:
                logger.info(f"Error while crawling permissions: {error}. Skipping")
                continue
            logger.error(f"Error while crawling permissions: {error}")
            acute_errors.append(error)
        if acute_errors:
            raise ManyError(acute_errors)
        logger.info(f"Total crawled permissions: {len(items)}")
        return items

    def apply_group_permissions(self, migration_state: MigrationState) -> bool:
        # list shall be sorted prior to using group by
        if len(migration_state) == 0:
            logger.info("No valid groups selected, nothing to do.")
            return True
        items = sorted(self.snapshot(), key=lambda i: i.object_type)
        logger.info(
            f"Applying the permissions to account groups. "
            f"Total groups to apply permissions: {len(migration_state)}. "
            f"Total permissions found: {len(items)}"
        )
        applier_tasks: list[Callable[..., None]] = []
        supports_to_items = {
            support: list(items_subset) for support, items_subset in groupby(items, key=lambda i: i.object_type)
        }

        appliers = self.object_type_support()

        # we first check that all supports are valid.
        for object_type in supports_to_items:
            if object_type not in appliers:
                msg = f"Could not find support for {object_type}. Please check the inventory table."
                raise ValueError(msg)

        for object_type, items_subset in supports_to_items.items():
            relevant_support = appliers[object_type]
            tasks_for_support: list[Callable[..., None]] = []
            for item in items_subset:
                if not item:
                    continue
                task = relevant_support.get_apply_task(item, migration_state)
                if not task:
                    continue
                tasks_for_support.append(task)
            if len(tasks_for_support) == 0:
                continue
            logger.info(f"Total tasks for {object_type}: {len(tasks_for_support)}")
            applier_tasks.extend(tasks_for_support)

        logger.info(f"Starting to apply permissions on account groups. Total tasks: {len(applier_tasks)}")

        _, errors = Threads.gather("apply account group permissions", applier_tasks)
        if len(errors) > 0:
            logger.error(f"Detected {len(errors)} while applying permissions")
            raise ManyError(errors)
        logger.info("Permissions were applied")
        return True

    def verify_group_permissions(self) -> bool:
        items = sorted(self.snapshot(), key=lambda i: i.object_type)
        logger.info(f"Total permissions found: {len(items)}")
        verifier_tasks: list[Callable[..., bool]] = []
        appliers = self.object_type_support()

        for object_type, items_subset in groupby(items, key=lambda i: i.object_type):
            if object_type not in appliers:
                msg = f"Could not find support for {object_type}. Please check the inventory table."
                raise ValueError(msg)

            relevant_support = appliers[object_type]
            tasks_for_support: list[Callable[..., bool]] = []
            for item in items_subset:
                task = relevant_support.get_verify_task(item)
                if not task:
                    continue
                tasks_for_support.append(task)

            logger.info(f"Total tasks for {object_type}: {len(tasks_for_support)}")
            verifier_tasks.extend(tasks_for_support)

        logger.info(f"Starting to verify permissions. Total tasks: {len(verifier_tasks)}")
        Threads.strict("verify group permissions", verifier_tasks)
        logger.info("All permissions validated successfully. No issues found.")

        return True

    def object_type_support(self) -> dict[str, AclSupport]:
        appliers: dict[str, AclSupport] = {}
        for support in self._acl_support:
            for object_type in support.object_types():
                if object_type in appliers:
                    msg = f"{object_type} is already supported by {type(appliers[object_type]).__name__}"
                    raise KeyError(msg)
                appliers[object_type] = support
        return appliers

    def _try_fetch(self) -> Iterable[Permissions]:
        for row in self._fetch(f"SELECT object_id, object_type, raw FROM {escape_sql_identifier(self.full_name)}"):
            yield Permissions(*row)

    def _get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        for support in self._acl_support:
            yield from support.get_crawler_tasks()
