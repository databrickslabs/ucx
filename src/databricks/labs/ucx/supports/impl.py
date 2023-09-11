from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from databricks.labs.ucx.inventory.listing import experiments_listing, models_listing
from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.supports.group_level import GroupLevelSupport
from databricks.labs.ucx.supports.passwords import PasswordsSupport
from databricks.labs.ucx.supports.permissions import (
    PermissionsSupport,
    WorkspaceSupport,
)
from databricks.labs.ucx.supports.secrets import SecretsSupport
from databricks.labs.ucx.supports.sql import SqlPermissionsSupport
from databricks.labs.ucx.supports.tokens import TokensSupport


def get_supports(ws: WorkspaceClient, num_threads: int, workspace_start_path: str):
    return {
        "entitlements": GroupLevelSupport(ws=ws, property_name="entitlements"),
        "roles": GroupLevelSupport(ws=ws, property_name="roles"),
        "clusters": PermissionsSupport(
            ws=ws, listing_function=ws.clusters.list, id_attribute="cluster_id", request_type=RequestObjectType.CLUSTERS
        ),
        "cluster_policies": PermissionsSupport(
            ws=ws,
            listing_function=ws.cluster_policies.list,
            id_attribute="cluster_policy_id",
            request_type=RequestObjectType.CLUSTER_POLICIES,
        ),
        "instance_pools": PermissionsSupport(
            ws=ws,
            listing_function=ws.instance_pools.list,
            id_attribute="instance_pool_id",
            request_type=RequestObjectType.INSTANCE_POOLS,
        ),
        "sql_warehouses": PermissionsSupport(
            ws=ws, listing_function=ws.warehouses.list, id_attribute="id", request_type=RequestObjectType.SQL_WAREHOUSES
        ),
        "jobs": PermissionsSupport(
            ws=ws, listing_function=ws.jobs.list, id_attribute="job_id", request_type=RequestObjectType.JOBS
        ),
        "pipelines": PermissionsSupport(
            ws=ws,
            listing_function=ws.pipelines.list,
            id_attribute="pipeline_id",
            request_type=RequestObjectType.PIPELINES,
        ),
        "experiments": PermissionsSupport(
            ws=ws,
            listing_function=experiments_listing(ws),
            id_attribute="experiment_id",
            request_type=RequestObjectType.EXPERIMENTS,
        ),
        "registered_models": PermissionsSupport(
            ws=ws,
            listing_function=models_listing(ws),
            id_attribute="id",
            request_type=RequestObjectType.REGISTERED_MODELS,
        ),
        "alerts": SqlPermissionsSupport(
            ws=ws, listing_function=ws.alerts.list, id_attribute="alert_id", object_type=sql.ObjectTypePlural.ALERTS
        ),
        "dashboards": SqlPermissionsSupport(
            ws=ws,
            listing_function=ws.dashboards.list,
            id_attribute="dashboard_id",
            object_type=sql.ObjectTypePlural.DASHBOARDS,
        ),
        "queries": SqlPermissionsSupport(
            ws=ws, listing_function=ws.queries.list, id_attribute="query_id", object_type=sql.ObjectTypePlural.QUERIES
        ),
        "tokens": TokensSupport(ws=ws),
        "passwords": PasswordsSupport(ws=ws),
        "secrets": SecretsSupport(ws),
        "workspace": WorkspaceSupport(ws=ws, num_threads=num_threads, start_path=workspace_start_path),
    }
