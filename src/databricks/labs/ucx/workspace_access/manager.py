import json
import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from itertools import groupby

from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import (
    CrawlerBase,
    Dataclass,
    DataclassInstance,
)
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


class PermissionManager(CrawlerBase[Permissions]):
    ERRORS_TO_IGNORE = ["FEATURE_DISABLED"]

    def __init__(self, backend: SqlBackend, inventory_database: str, crawlers: list[AclSupport]):
        super().__init__(backend, "hive_metastore", inventory_database, "permissions", Permissions)
        self._acl_support = crawlers

    def inventorize_permissions(self):
        # TODO: rename into snapshot()
        logger.debug("Crawling permissions")
        crawler_tasks = list(self._get_crawler_tasks())
        logger.info(f"Starting to crawl permissions. Total tasks: {len(crawler_tasks)}")
        items, errors = Threads.gather("crawl permissions", crawler_tasks)
        acute_errors = []
        for error in errors:
            if error.error_code not in self.ERRORS_TO_IGNORE:
                logger.error(f"Error while crawling permissions: {error}")
                acute_errors.append(error)
                continue
            logger.info(f"Error while crawling permissions: {error}. Skipping")
        if len(acute_errors) > 0:
            raise ManyError(acute_errors)
        logger.info(f"Total crawled permissions: {len(items)}")
        self._save(items)
        logger.info(f"Saved {len(items)} to {self.full_name}")

    def apply_group_permissions(self, migration_state: MigrationState) -> bool:
        # list shall be sorted prior to using group by
        if len(migration_state) == 0:
            logger.info("No valid groups selected, nothing to do.")
            return True
        items = sorted(self.load_all(), key=lambda i: i.object_type)
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
        items = sorted(self.load_all(), key=lambda i: i.object_type)
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

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self.full_name}")
        self._exec(f"DROP TABLE IF EXISTS {self.full_name}")
        logger.info("Inventory table cleanup complete")

    def _save(self, items: Sequence[Permissions]):
        # keep in mind, that object_type and object_id are not primary keys.
        self._append_records(items)  # TODO: update instead of append
        logger.info("Successfully saved the items to inventory table")

    def load_all(self) -> list[Permissions]:
        logger.info(f"Loading inventory table {self.full_name}")
        if list(self._fetch(f"SELECT COUNT(*) as cnt FROM {self.full_name}"))[0][0] == 0:  # noqa: RUF015
            msg = (
                f"table {self.full_name} is empty for fetching permission info. "
                f"Please ensure assessment job is run successfully and permissions populated"
            )
            raise RuntimeError(msg)
        return [
            Permissions(object_id, object_type, raw)
            for object_id, object_type, raw in self._fetch(f"SELECT object_id, object_type, raw FROM {self.full_name}")
        ]

    def load_all_for(self, object_type: str, object_id: str, klass: Dataclass) -> Iterable[DataclassInstance]:
        for perm in self.load_all():
            if object_type == perm.object_type and object_id.lower() == perm.object_id.lower():
                raw = json.loads(perm.raw)
                yield klass(**raw)

    def _get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        for support in self._acl_support:
            yield from support.get_crawler_tasks()
