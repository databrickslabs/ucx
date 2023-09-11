from collections.abc import Callable

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.support.listing import experiments_listing, models_listing
from databricks.labs.ucx.inventory.types import PermissionsInventoryItem, RequestObjectType
from databricks.labs.ucx.support.base import BaseSupport
from databricks.labs.ucx.support.group_level import ScimSupport
from databricks.labs.ucx.support.permissions import GenericPermissionsSupport, listing_wrapper
from databricks.labs.ucx.support.secrets import SecretScopesSupport
from databricks.labs.ucx.support.sql import get_sql_support


class SupportsProvider:
    def __init__(self, ws: WorkspaceClient, num_threads: int, workspace_start_path: str):
        self._generic_support = GenericPermissionsSupport(
            ws=ws,
            listings=[
                listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
                listing_wrapper(ws.cluster_policies.list, "cluster_policy_id", RequestObjectType.CLUSTER_POLICIES),
                listing_wrapper(ws.instance_pools.list, "instance_pool_id", RequestObjectType.INSTANCE_POOLS),
                listing_wrapper(ws.warehouses.list, "id", RequestObjectType.SQL_WAREHOUSES),
                listing_wrapper(ws.jobs.list, "job_id", RequestObjectType.JOBS),
                listing_wrapper(ws.pipelines.list, "pipeline_id", RequestObjectType.PIPELINES),
                listing_wrapper(experiments_listing(ws), "experiment_id", RequestObjectType.EXPERIMENTS),
                listing_wrapper(models_listing(ws), "id", RequestObjectType.REGISTERED_MODELS),
                _workspace_listing(ws, num_threads=num_threads, start_path=start_path),
                authorization_listing(),
            ],
        )
        self._secrets_support = SecretScopesSupport(ws=ws)
        self._scim_support = ScimSupport(ws)
        self._sql_support = get_sql_support(ws=ws)

    def get_crawler_tasks(self) -> list[Callable[..., PermissionsInventoryItem | None]]:
        for support in [self._generic_support, self._secrets_support, self._scim_support, self._sql_support]:
            yield from support.get_crawler_tasks()

    @property
    def supports(self) -> dict[str, BaseSupport]:
        return {
            # SCIM-based API
            "entitlements": self._scim_support,
            "roles": self._scim_support,
            # generic API
            "clusters": self._generic_support,
            "cluster_policies": self._generic_support,
            "instance_pools": self._generic_support,
            "sql_warehouses": self._generic_support,
            "jobs": self._generic_support,
            "pipelines": self._generic_support,
            "experiments": self._generic_support,
            "registered_models": self._generic_support,
            "tokens": self._generic_support,
            "passwords": self._generic_support,
            # workspace objects
            "notebooks": self._generic_support,
            "files": self._generic_support,
            "directories": self._generic_support,
            "repos": self._generic_support,
            # SQL API
            "alerts": self._sql_support,
            "queries": self._sql_support,
            "dashboards": self._sql_support,
            # secrets API
            "secrets": self._secrets_support,
        }
