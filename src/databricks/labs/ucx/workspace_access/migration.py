import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.framework.crawlers import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.workspace_access.base import RequestObjectType
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport
from databricks.labs.ucx.workspace_access.verification import VerificationManager

from databricks.labs.ucx.workspace_access.generic import GenericPermissionsSupport, authorization_listing, experiments_listing, listing_wrapper, models_listing, workspace_listing
from databricks.labs.ucx.workspace_access.redash import SqlPermissionsSupport, redash_listing_wrapper
from databricks.labs.ucx.workspace_access.scim import ScimSupport


class GroupMigrationToolkit:
    def __init__(self, config: MigrationConfig, *, warehouse_id=None):
        self._configure_logger(config.log_level)

        ws = WorkspaceClient(config=config.to_databricks_config())
        ws.api_client._session.adapters["https://"].max_retries.total = 20
        self._verify_ws_client(ws)

        generic_acl_listing = [
            listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
            listing_wrapper(ws.cluster_policies.list, "policy_id", RequestObjectType.CLUSTER_POLICIES),
            listing_wrapper(ws.instance_pools.list, "instance_pool_id", RequestObjectType.INSTANCE_POOLS),
            listing_wrapper(ws.warehouses.list, "id", RequestObjectType.SQL_WAREHOUSES),
            listing_wrapper(ws.jobs.list, "job_id", RequestObjectType.JOBS),
            listing_wrapper(ws.pipelines.list_pipelines, "pipeline_id", RequestObjectType.PIPELINES),
            listing_wrapper(experiments_listing(ws), "experiment_id", RequestObjectType.EXPERIMENTS),
            listing_wrapper(models_listing(ws), "id", RequestObjectType.REGISTERED_MODELS),
            workspace_listing(ws, num_threads=config.num_threads, start_path=config.workspace_start_path),
            authorization_listing(),
        ]
        redash_acl_listing = [
            redash_listing_wrapper(ws.alerts.list, sql.ObjectTypePlural.ALERTS),
            redash_listing_wrapper(ws.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
            redash_listing_wrapper(ws.queries.list, sql.ObjectTypePlural.QUERIES),
        ]
        generic_support = GenericPermissionsSupport(ws, generic_acl_listing)
        sql_support = SqlPermissionsSupport(ws, redash_acl_listing)
        secrets_support = SecretScopesSupport(ws)
        scim_support = ScimSupport(ws)
        self._permissions_manager = PermissionManager(
            self._backend(ws, warehouse_id),
            config.inventory_database,
            [generic_support, sql_support, secrets_support, scim_support],
            self._object_type_appliers(generic_support, sql_support, secrets_support, scim_support),
        )
        self._group_manager = GroupManager(ws, config.groups)
        self._verification_manager = VerificationManager(ws, secrets_support)

    @staticmethod
    def _object_type_appliers(generic_support, sql_support, secrets_support, scim_support):
        return {
            # SCIM-based API
            "entitlements": scim_support,
            "roles": scim_support,
            # generic API
            "clusters": generic_support,
            "cluster-policies": generic_support,
            "instance-pools": generic_support,
            "sql/warehouses": generic_support,
            "jobs": generic_support,
            "pipelines": generic_support,
            "experiments": generic_support,
            "registered-models": generic_support,
            "tokens": generic_support,
            "passwords": generic_support,
            # workspace objects
            "notebooks": generic_support,
            "files": generic_support,
            "directories": generic_support,
            "repos": generic_support,
            # SQL API
            "alerts": sql_support,
            "queries": sql_support,
            "dashboards": sql_support,
            # secrets API
            "secrets": secrets_support,
        }

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    @staticmethod
    def _configure_logger(level: str):
        ucx_logger = logging.getLogger("databricks.labs.ucx")
        ucx_logger.setLevel(level)

    def prepare_environment(self):
        self._group_manager.prepare_groups_in_environment()

    def cleanup_inventory_table(self):
        self._permissions_manager.cleanup()

    def inventorize_permissions(self):
        self._permissions_manager.inventorize_permissions()

    def apply_permissions_to_backup_groups(self):
        self._permissions_manager.apply_group_permissions(
            self._group_manager.migration_groups_provider, destination="backup"
        )

    def verify_permissions_on_backup_groups(self, to_verify):
        self._verification_manager.verify(self._group_manager.migration_groups_provider, "backup", to_verify)

    def replace_workspace_groups_with_account_groups(self):
        self._group_manager.replace_workspace_groups_with_account_groups()

    def apply_permissions_to_account_groups(self):
        self._permissions_manager.apply_group_permissions(
            self._group_manager.migration_groups_provider, destination="account"
        )

    def verify_permissions_on_account_groups(self, to_verify):
        self._verification_manager.verify(self._group_manager.migration_groups_provider, "account", to_verify)

    def delete_backup_groups(self):
        self._group_manager.delete_backup_groups()
