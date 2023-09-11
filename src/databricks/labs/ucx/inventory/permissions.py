import logging
from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import (
    LogicalObjectType,
    PermissionsInventoryItem,
)
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.supports.base import BaseSupport
from databricks.labs.ucx.utils import ThreadedExecution

logger = logging.getLogger(__name__)


class PermissionManager:
    def __init__(self, ws: WorkspaceClient, permissions_inventory: PermissionsInventoryTable):
        self._ws = ws
        self._permissions_inventory = permissions_inventory
        self._supports: dict[str, BaseSupport] = {}

    @property
    def supports(self) -> dict[str, BaseSupport]:
        return self._supports

    def set_supports(self, supports: dict[str, BaseSupport]):
        self._supports = supports

    def inventorize_permissions(self):
        logger.info("Inventorizing the permissions")
        crawler_tasks = []

        for name, support in self._supports.items():
            logger.info(f"Adding crawler tasks for {name}")
            crawler_tasks.extend(support.get_crawler_tasks())
            logger.info(f"Added crawler tasks for {name}")

        logger.info(f"Total crawler tasks: {len(crawler_tasks)}")
        logger.info("Starting the permissions inventorization")
        execution = ThreadedExecution[PermissionsInventoryItem | None](crawler_tasks)
        results = execution.run()
        items = [item for item in results if item is not None]
        logger.info(f"Total inventorized items: {len(items)}")
        self._permissions_inventory.save(items)
        logger.info("Permissions were inventorized and saved")

    def apply_group_permissions(self, migration_state: GroupMigrationState, destination: Literal["backup", "account"]):
        logger.info(f"Applying the permissions to {destination} groups")
        logger.info(f"Total groups to apply permissions: {len(migration_state.groups)}")
        items = self._permissions_inventory.load_all()
        logger.info(f"Total inventorized items: {len(items)}")
        applier_tasks = []
        for name, _support in self._supports.items():
            logger.info(f"Adding applier tasks for {name}")
            applier_tasks.extend(
                [self._supports.get(item.crawler).get_apply_task(item, migration_state, destination) for item in items]
            )
            logger.info(f"Added applier tasks for {name}")

        logger.info(f"Total applier tasks: {len(applier_tasks)}")
        logger.info("Starting the permissions application")
        execution = ThreadedExecution(applier_tasks)
        execution.run()
        logger.info("Permissions were applied")

    def verify(
        self, migration_state: GroupMigrationState, target: Literal["backup", "account"], tuples: list[tuple[str, str]]
    ):
        for object_type, object_id in tuples:
            if object_type == LogicalObjectType.SECRET_SCOPE:
                self.verify_applied_scope_acls(object_id, migration_state, target)
            else:
                self.verify_applied_permissions(object_type, object_id, migration_state, target)
        self.verify_roles_and_entitlements(migration_state, target)

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

    def verify_roles_and_entitlements(self, migration_state: GroupMigrationState, target: Literal["backup", "account"]):
        for el in migration_state.groups:
            comparison_base = getattr(el, "workspace" if target == "backup" else "backup")
            comparison_target = getattr(el, target)

            base_group_info = self._ws.groups.get(comparison_base.id)
            target_group_info = self._ws.groups.get(comparison_target.id)

            assert base_group_info.roles == target_group_info.roles
            assert base_group_info.entitlements == target_group_info.entitlements

    def _secret_scope_permission(self, scope_name: str, group_name: str) -> workspace.AclPermission | None:
        for acl in self._ws.secrets.list_acls(scope=scope_name):
            if acl.principal == group_name:
                return acl.permission
        return None
