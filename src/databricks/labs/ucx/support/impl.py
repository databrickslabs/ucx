from collections.abc import Callable, Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from databricks.labs.ucx.inventory.types import (
    PermissionsInventoryItem,
    RequestObjectType,
)
from databricks.labs.ucx.support.base import BaseSupport
from databricks.labs.ucx.support.group_level import ScimSupport
from databricks.labs.ucx.support.listing import (
    authorization_listing,
    experiments_listing,
    models_listing,
    workspace_listing,
)
from databricks.labs.ucx.support.permissions import (
    GenericPermissionsSupport,
    listing_wrapper,
)
from databricks.labs.ucx.support.secrets import SecretScopesSupport
from databricks.labs.ucx.support.sql import SqlPermissionsSupport
from databricks.labs.ucx.support.sql import listing_wrapper as sql_listing_wrapper


class SupportsProvider:
    def __init__(self, ws: WorkspaceClient, num_threads: int, workspace_start_path: str):
        self._generic_support = GenericPermissionsSupport(
            ws=ws,
            listings=[
                listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
                listing_wrapper(ws.cluster_policies.list, "policy_id", RequestObjectType.CLUSTER_POLICIES),
                listing_wrapper(ws.instance_pools.list, "instance_pool_id", RequestObjectType.INSTANCE_POOLS),
                listing_wrapper(ws.warehouses.list, "id", RequestObjectType.SQL_WAREHOUSES),
                listing_wrapper(ws.jobs.list, "job_id", RequestObjectType.JOBS),
                listing_wrapper(ws.pipelines.list_pipelines, "pipeline_id", RequestObjectType.PIPELINES),
                listing_wrapper(experiments_listing(ws), "experiment_id", RequestObjectType.EXPERIMENTS),
                listing_wrapper(models_listing(ws), "id", RequestObjectType.REGISTERED_MODELS),
                workspace_listing(ws, num_threads=num_threads, start_path=workspace_start_path),
                authorization_listing(),
            ],
        )
        self._secrets_support = SecretScopesSupport(ws=ws)
        self._scim_support = ScimSupport(ws)
        self._sql_support = SqlPermissionsSupport(
            ws,
            listings=[
                sql_listing_wrapper(ws.alerts.list, sql.ObjectTypePlural.ALERTS),
                sql_listing_wrapper(ws.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
                sql_listing_wrapper(ws.queries.list, sql.ObjectTypePlural.QUERIES),
            ],
        )

    def get_crawler_tasks(self) -> Iterator[Callable[..., PermissionsInventoryItem | None]]:
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
            "cluster-policies": self._generic_support,
            "instance-pools": self._generic_support,
            "sql/warehouses": self._generic_support,
            "jobs": self._generic_support,
            "pipelines": self._generic_support,
            "experiments": self._generic_support,
            "registered-models": self._generic_support,
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
