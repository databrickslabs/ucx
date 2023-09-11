from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from databricks.labs.ucx.inventory.listing import experiments_listing, models_listing
from databricks.labs.ucx.inventory.types import RequestObjectType, Supports
from databricks.labs.ucx.supports.base import BaseSupport
from databricks.labs.ucx.supports.group_level import GroupLevelSupport
from databricks.labs.ucx.supports.passwords import PasswordsSupport
from databricks.labs.ucx.supports.permissions import (
    PermissionsSupport,
    WorkspaceSupport,
)
from databricks.labs.ucx.supports.secrets import SecretsSupport
from databricks.labs.ucx.supports.sql import SqlPermissionsSupport
from databricks.labs.ucx.supports.tokens import TokensSupport


def get_supports(ws: WorkspaceClient, num_threads: int, workspace_start_path: str) -> dict[Supports, BaseSupport]:
    return {
        Supports.entitlements: GroupLevelSupport(ws=ws, support_name=Supports.entitlements),
        Supports.roles: GroupLevelSupport(ws=ws, support_name=Supports.roles),
        Supports.clusters: PermissionsSupport(
            support_name=Supports.clusters,
            ws=ws,
            listing_function=ws.clusters.list,
            id_attribute="cluster_id",
            request_type=RequestObjectType.CLUSTERS,
        ),
        Supports.cluster_policies: PermissionsSupport(
            ws=ws,
            support_name=Supports.cluster_policies,
            listing_function=ws.cluster_policies.list,
            id_attribute="cluster_policy_id",
            request_type=RequestObjectType.CLUSTER_POLICIES,
        ),
        Supports.instance_pools: PermissionsSupport(
            ws=ws,
            support_name=Supports.instance_pools,
            listing_function=ws.instance_pools.list,
            id_attribute="instance_pool_id",
            request_type=RequestObjectType.INSTANCE_POOLS,
        ),
        Supports.sql_warehouses: PermissionsSupport(
            support_name=Supports.sql_warehouses,
            ws=ws,
            listing_function=ws.warehouses.list,
            id_attribute="id",
            request_type=RequestObjectType.SQL_WAREHOUSES,
        ),
        Supports.jobs: PermissionsSupport(
            support_name=Supports.jobs,
            ws=ws,
            listing_function=ws.jobs.list,
            id_attribute="job_id",
            request_type=RequestObjectType.JOBS,
        ),
        Supports.pipelines: PermissionsSupport(
            ws=ws,
            support_name=Supports.pipelines,
            listing_function=ws.pipelines.list,
            id_attribute="pipeline_id",
            request_type=RequestObjectType.PIPELINES,
        ),
        Supports.experiments: PermissionsSupport(
            ws=ws,
            support_name=Supports.experiments,
            listing_function=experiments_listing(ws),
            id_attribute="experiment_id",
            request_type=RequestObjectType.EXPERIMENTS,
        ),
        Supports.registered_models: PermissionsSupport(
            ws=ws,
            support_name=Supports.registered_models,
            listing_function=models_listing(ws),
            id_attribute="id",
            request_type=RequestObjectType.REGISTERED_MODELS,
        ),
        Supports.alerts: SqlPermissionsSupport(
            support_name=Supports.alerts,
            ws=ws,
            listing_function=ws.alerts.list,
            id_attribute="alert_id",
            object_type=sql.ObjectTypePlural.ALERTS,
        ),
        Supports.dashboards: SqlPermissionsSupport(
            ws=ws,
            support_name=Supports.dashboards,
            listing_function=ws.dashboards.list,
            id_attribute="dashboard_id",
            object_type=sql.ObjectTypePlural.DASHBOARDS,
        ),
        Supports.queries: SqlPermissionsSupport(
            support_name=Supports.queries,
            ws=ws,
            listing_function=ws.queries.list,
            id_attribute="query_id",
            object_type=sql.ObjectTypePlural.QUERIES,
        ),
        Supports.tokens: TokensSupport(ws=ws, support_name=Supports.tokens),
        Supports.passwords: PasswordsSupport(ws=ws, support_name=Supports.passwords),
        Supports.secrets: SecretsSupport(ws, support_name=Supports.secrets),
        Supports.workspace: WorkspaceSupport(
            ws=ws, num_threads=num_threads, start_path=workspace_start_path, support_name=Supports.workspace
        ),
    }
