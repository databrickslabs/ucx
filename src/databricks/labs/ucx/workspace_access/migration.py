import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport
from databricks.labs.ucx.workspace_access.verification import VerificationManager


# TODO: fully remove this obsolete class later
class GroupMigrationToolkit:
    def __init__(self, config: WorkspaceConfig, *, warehouse_id=None):
        self._configure_logger(config.log_level)

        ws = WorkspaceClient(config=config.to_databricks_config())
        self._verify_ws_client(ws)

        secrets_support = SecretScopesSupport(ws)
        self._permissions_manager = PermissionManager.factory(
            ws,
            self._backend(ws, warehouse_id),
            config.inventory_database,
            num_threads=config.num_threads,
            workspace_start_path=config.workspace_start_path,
        )
        self._group_manager = GroupManager(ws, config.groups)
        # TODO: remove VerificationManager in scope of https://github.com/databrickslabs/ucx/issues/36,
        # where we probably should add verify() abstract method to every Applier
        self._verification_manager = VerificationManager(ws, secrets_support)

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

    def has_groups(self) -> bool:
        return self._group_manager.has_groups()

    def prepare_environment(self):
        self._group_manager.prepare_groups_in_environment()

    def cleanup_inventory_table(self):
        self._permissions_manager.cleanup()

    def inventorize_permissions(self):
        self._permissions_manager.inventorize_permissions()

    def apply_permissions_to_backup_groups(self):
        self._permissions_manager.apply_group_permissions(self._group_manager.migration_state, destination="backup")

    def verify_permissions_on_backup_groups(self, to_verify):
        self._verification_manager.verify(self._group_manager.migration_state, "backup", to_verify)

    def replace_workspace_groups_with_account_groups(self):
        self._group_manager.replace_workspace_groups_with_account_groups()

    def apply_permissions_to_account_groups(self):
        self._permissions_manager.apply_group_permissions(self._group_manager.migration_state, destination="account")

    def verify_permissions_on_account_groups(self, to_verify):
        self._verification_manager.verify(self._group_manager.migration_state, "account", to_verify)

    def delete_backup_groups(self):
        self._group_manager.delete_backup_groups()
