import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.framework.crawlers import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.mounts.list_mounts import MountLister

from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    authorization_listing,
    experiments_listing,
    listing_wrapper,
    models_listing,
    workspace_listing,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.redash import (
    SqlPermissionsSupport,
    redash_listing_wrapper,
)
from databricks.labs.ucx.workspace_access.scim import ScimSupport
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport
from databricks.labs.ucx.workspace_access.verification import VerificationManager


class GroupMigrationToolkit:
    def __init__(self, config: MigrationConfig, *, warehouse_id=None):
        self._configure_logger(config.log_level)

        ws = WorkspaceClient(config=config.to_databricks_config())
        ws.api_client._session.adapters["https://"].max_retries.total = 20
        self._verify_ws_client(ws)
        self._ws = ws  # TODO: remove this once notebooks/toolkit.py is removed

        generic_acl_listing = [
            listing_wrapper(ws.clusters.list, "cluster_id", "clusters"),
            listing_wrapper(ws.cluster_policies.list, "policy_id", "cluster-policies"),
            listing_wrapper(ws.instance_pools.list, "instance_pool_id", "instance-pools"),
            listing_wrapper(ws.warehouses.list, "id", "sql/warehouses"),
            listing_wrapper(ws.jobs.list, "job_id", "jobs"),
            listing_wrapper(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
            listing_wrapper(experiments_listing(ws), "experiment_id", "experiments"),
            listing_wrapper(models_listing(ws), "id", "registered-models"),
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
        self._mount_lister = MountLister(ws, config.inventory_database)

    @staticmethod
    def _object_type_appliers(generic_support, sql_support, secrets_support, scim_support):
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

    def inventorize_mounts(self):
        self._mount_lister.inventorize_mounts()
