import json
import logging
import os
from collections.abc import Callable, Iterator
from itertools import groupby
from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.workspace_access import generic, redash, scim, secrets
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

logger = logging.getLogger(__name__)


class PermissionManager(CrawlerBase):
    def __init__(self, backend: SqlBackend, inventory_database: str, crawlers: list[AclSupport]):
        super().__init__(backend, "hive_metastore", inventory_database, "permissions", Permissions)
        self._acl_support = crawlers

    @classmethod
    def factory(
        cls,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        inventory_database: str,
        *,
        num_threads: int | None = None,
        workspace_start_path: str = "/",
    ) -> "PermissionManager":
        if num_threads is None:
            num_threads = os.cpu_count() * 2
        generic_acl_listing = [
            generic.Listing(ws.clusters.list, "cluster_id", "clusters"),
            generic.Listing(ws.cluster_policies.list, "policy_id", "cluster-policies"),
            generic.Listing(ws.instance_pools.list, "instance_pool_id", "instance-pools"),
            generic.Listing(ws.warehouses.list, "id", "sql/warehouses"),
            generic.Listing(ws.jobs.list, "job_id", "jobs"),
            generic.Listing(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
            generic.Listing(generic.experiments_listing(ws), "experiment_id", "experiments"),
            generic.Listing(generic.models_listing(ws), "id", "registered-models"),
            generic.Listing(generic.tokens_and_passwords, "object_id", "authorization"),
            generic.WorkspaceListing(
                ws,
                sql_backend=sql_backend,
                inventory_database=inventory_database,
                num_threads=num_threads,
                start_path=workspace_start_path,
            ),
        ]
        redash_acl_listing = [
            redash.Listing(ws.alerts.list, sql.ObjectTypePlural.ALERTS),
            redash.Listing(ws.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
            redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES),
        ]
        generic_support = generic.GenericPermissionsSupport(ws, generic_acl_listing)
        sql_support = redash.RedashPermissionsSupport(ws, redash_acl_listing)
        secrets_support = secrets.SecretScopesSupport(ws)
        scim_support = scim.ScimSupport(ws)
        tables_crawler = TablesCrawler(sql_backend, inventory_database)
        grants_crawler = GrantsCrawler(tables_crawler)
        tacl_support = TableAclSupport(grants_crawler, sql_backend)
        return cls(
            sql_backend, inventory_database, [generic_support, sql_support, secrets_support, scim_support, tacl_support]
        )

    def inventorize_permissions(self):
        logger.debug("Crawling permissions")
        crawler_tasks = list(self._get_crawler_tasks())
        logger.info(f"Starting to crawl permissions. Total tasks: {len(crawler_tasks)}")
        items, errors = Threads.gather("crawl permissions", crawler_tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Detected {len(errors)} errors while crawling permissions")
        logger.info(f"Total crawled permissions: {len(items)}")
        self._save(items)
        logger.info(f"Saved {len(items)} to {self._full_name}")

    def apply_group_permissions(self, migration_state: GroupMigrationState, destination: Literal["backup", "account"]):
        # list shall be sorted prior to using group by
        if len(migration_state) == 0:
            logger.info("No valid groups selected, nothing to do.")
            return True
        items = sorted(self.load_all(), key=lambda i: i.object_type)
        logger.info(
            f"Applying the permissions to {destination} groups. "
            f"Total groups to apply permissions: {len(migration_state)}. "
            f"Total permissions found: {len(items)}"
        )
        applier_tasks = []
        supports_to_items = {
            support: list(items_subset) for support, items_subset in groupby(items, key=lambda i: i.object_type)
        }

        appliers = self._appliers()

        # we first check that all supports are valid.
        for object_type in supports_to_items:
            if object_type not in appliers:
                msg = f"Could not find support for {object_type}. Please check the inventory table."
                raise ValueError(msg)

        for object_type, items_subset in supports_to_items.items():
            relevant_support = appliers[object_type]
            tasks_for_support = [
                relevant_support.get_apply_task(item, migration_state, destination) for item in items_subset
            ]
            tasks_for_support = [_ for _ in tasks_for_support if _ is not None]
            if len(tasks_for_support) == 0:
                continue
            logger.info(f"Total tasks for {object_type}: {len(tasks_for_support)}")
            applier_tasks.extend(tasks_for_support)

        logger.info(f"Starting to apply permissions on {destination} groups. Total tasks: {len(applier_tasks)}")
        _, errors = Threads.gather(f"apply {destination} group permissions", applier_tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Detected {len(errors)} while applying permissions")
            return False
        logger.info("Permissions were applied")
        return True

    def _appliers(self) -> dict[str, AclSupport]:
        appliers = {}
        for support in self._acl_support:
            for object_type in support.object_types():
                if object_type in appliers:
                    msg = f"{object_type} is already supported by {type(appliers[object_type]).__name__}"
                    raise KeyError(msg)
                appliers[object_type] = support
        return appliers

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self._full_name}")
        self._exec(f"DROP TABLE IF EXISTS {self._full_name}")
        logger.info("Inventory table cleanup complete")

    def _save(self, items: list[Permissions]):
        # keep in mind, that object_type and object_id are not primary keys.
        self._append_records(items)  # TODO: update instead of append
        logger.info("Successfully saved the items to inventory table")

    def load_all(self) -> list[Permissions]:
        logger.info(f"Loading inventory table {self._full_name}")
        return [
            Permissions(object_id, object_type, raw)
            for object_id, object_type, raw in self._fetch(f"SELECT object_id, object_type, raw FROM {self._full_name}")
        ]

    def load_all_for(self, object_type: str, object_id: str, klass: type) -> any:
        for perm in self.load_all():
            if object_type == perm.object_type and object_id == perm.object_id:
                raw = json.loads(perm.raw)
                yield klass(**raw)

    def _get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        for support in self._acl_support:
            yield from support.get_crawler_tasks()
