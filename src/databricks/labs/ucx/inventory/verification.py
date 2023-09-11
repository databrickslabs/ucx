from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

from databricks.labs.ucx.providers.groups_info import GroupMigrationState


class VerificationManager:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

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
