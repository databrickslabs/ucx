import logging
from typing import Literal

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.inventory.applicator import Applicators
from databricks.labs.ucx.inventory.inventorizer import BaseInventorizer
from databricks.labs.ucx.inventory.table import InventoryTableManager
from databricks.labs.ucx.inventory.types import Destination
from databricks.labs.ucx.providers.groups_info import GroupMigrationState

logger = logging.getLogger(__name__)


class PermissionManager:
    def __init__(self, ws: WorkspaceClient, inventory_table_manager: InventoryTableManager):
        self._ws = ws
        self.inventory_table_manager = inventory_table_manager
        self._inventorizers = []

    @property
    def inventorizers(self) -> list[BaseInventorizer]:
        return self._inventorizers

    def set_inventorizers(self, value: list[BaseInventorizer]):
        self._inventorizers = value

    def inventorize_permissions(self):
        for inventorizer in self.inventorizers:
            logger.info(f"Inventorizing the permissions for objects of type(s) {inventorizer.logical_object_types}")
            inventorizer.preload()
            collected = inventorizer.inventorize()
            if collected:
                self.inventory_table_manager.save(collected)
            else:
                logger.warning(f"No objects of type {inventorizer.logical_object_types} were found")

        logger.info("Permissions were inventorized and saved")

    def apply_group_permissions(self, migration_state: GroupMigrationState, destination: Destination):
        logger.info(f"Applying the permissions to {destination} groups")
        logger.info(f"Total groups to apply permissions: {len(migration_state.groups)}")

        permissions_on_source = self.inventory_table_manager.load_for_groups(
            groups=[g.workspace.display_name for g in migration_state.groups]
        )
        logger.info(f"Total permissions to apply: {len(permissions_on_source)}")

        applicators = Applicators(self._ws, migration_state, destination)
        applicators.prepare(permissions_on_source)  # this method is lightweight
        applicators.apply()  # this method is heavy and starts the threaded execution
        logger.info(f"All permissions were applied for {destination} groups")

    def verify_applied_permissions(
        self,
        object_type: str,
        object_id: str,
        migration_state: GroupMigrationState,
        target: Literal["backup", "account"],
    ):
        op = self._ws.permissions.get(object_type, object_id)
        for info in migration_state.groups:
            src_permissions = sorted(
                [_ for _ in op.access_control_list if _.group_name == info.workspace.display_name],
                key=lambda p: p.group_name,
            )
            dst_permissions = sorted(
                [_ for _ in op.access_control_list if _.group_name == getattr(info, target).display_name],
                key=lambda p: p.group_name,
            )
            assert len(dst_permissions) == len(
                src_permissions
            ), f"Target permissions were not applied correctly for {object_type}/{object_id}"
            assert [t.all_permissions for t in dst_permissions] == [
                s.all_permissions for s in src_permissions
            ], f"Target permissions were not applied correctly for {object_type}/{object_id}"

    def verify_applied_scope_acls(
        self, scope_name: str, migration_state: GroupMigrationState, target: Literal["backup", "account"]
    ):
        base_attr = "workspace" if target == "backup" else "backup"
        for mi in migration_state.groups:
            src_name = getattr(mi, base_attr).display_name
            dst_name = getattr(mi, target).display_name
            src_permission = self._secret_scope_permission(scope_name, src_name)
            dst_permission = self._secret_scope_permission(scope_name, dst_name)
            assert src_permission == dst_permission, "Scope ACLs were not applied correctly"

    def _secret_scope_permission(self, scope_name: str, group_name: str) -> workspace.AclPermission | None:
        for acl in self._ws.secrets.list_acls(scope=scope_name):
            if acl.principal == group_name:
                return acl.permission
        return None
