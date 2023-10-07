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
from databricks.labs.ucx.workspace_access.base import Applier, Crawler, Permissions
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

logger = logging.getLogger(__name__)


class PermissionManager(CrawlerBase):
    def __init__(
        self, backend: SqlBackend, inventory_database: str, crawlers: list[Crawler], appliers: dict[str, Applier]
    ):
        super().__init__(backend, "hive_metastore", inventory_database, "permissions", Permissions)
        self._crawlers = crawlers
        self._appliers = appliers

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
            generic.listing_wrapper(ws.clusters.list, "cluster_id", "clusters"),
            generic.listing_wrapper(ws.cluster_policies.list, "policy_id", "cluster-policies"),
            generic.listing_wrapper(ws.instance_pools.list, "instance_pool_id", "instance-pools"),
            generic.listing_wrapper(ws.warehouses.list, "id", "sql/warehouses"),
            generic.listing_wrapper(ws.jobs.list, "job_id", "jobs"),
            generic.listing_wrapper(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
            generic.listing_wrapper(generic.experiments_listing(ws), "experiment_id", "experiments"),
            generic.listing_wrapper(generic.models_listing(ws), "id", "registered-models"),
            generic.workspace_listing(ws, num_threads=num_threads, start_path=workspace_start_path),
            generic.authorization_listing(),
        ]
        redash_acl_listing = [
            redash.redash_listing_wrapper(ws.alerts.list, sql.ObjectTypePlural.ALERTS),
            redash.redash_listing_wrapper(ws.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
            redash.redash_listing_wrapper(ws.queries.list, sql.ObjectTypePlural.QUERIES),
        ]
        generic_support = generic.GenericPermissionsSupport(ws, generic_acl_listing)
        sql_support = redash.SqlPermissionsSupport(ws, redash_acl_listing)
        secrets_support = secrets.SecretScopesSupport(ws)
        scim_support = scim.ScimSupport(ws)
        tables_crawler = TablesCrawler(sql_backend, inventory_database)
        grants_crawler = GrantsCrawler(tables_crawler)
        tacl_support = TableAclSupport(grants_crawler, sql_backend)
        return cls(
            sql_backend,
            inventory_database,
            [generic_support, sql_support, secrets_support, scim_support, tacl_support],
            cls._object_type_appliers(generic_support, sql_support, secrets_support, scim_support, tacl_support),
        )

    @staticmethod
    def _object_type_appliers(generic_support, sql_support, secrets_support, scim_support, tacl_support):
        return {
            # SCIM-based API
            "entitlements": scim_support,
            "roles": scim_support,
            # Generic Permissions API
            "authorization": generic_support,
            "clusters": generic_support,
            "cluster-policies": generic_support,
            "instance-pools": generic_support,
            "sql/warehouses": generic_support,
            "jobs": generic_support,
            "pipelines": generic_support,
            "experiments": generic_support,
            "registered-models": generic_support,
            "notebooks": generic_support,
            "files": generic_support,
            "directories": generic_support,
            "repos": generic_support,
            # Redash equivalent of Generic Permissions API
            "alerts": sql_support,
            "queries": sql_support,
            "dashboards": sql_support,
            # Secret Scope ACL API
            "secrets": secrets_support,
            # Legacy Table ACLs
            "TABLE": tacl_support,
            "VIEW": tacl_support,
            "DATABASE": tacl_support,
            "ANY FILE": tacl_support,
            "ANONYMOUS FUNCTION": tacl_support,
            "CATALOG": tacl_support,
        }

    def inventorize_permissions(self):
        logger.debug("Crawling permissions")
        crawler_tasks = list(self._get_crawler_tasks())
        logger.info(f"Starting to crawl permissions. Total tasks: {len(crawler_tasks)}")
        results, errors = Threads.gather("crawl permissions", crawler_tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Detected {len(errors)} while crawling permissions")
        items = []
        for item in results:
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
            return True
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
        _, errors = Threads.gather(f"apply {destination} group permissions", applier_tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Detected {len(errors)} while applying permissions")
            return False
        logger.info("Permissions were applied")
        return True

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
