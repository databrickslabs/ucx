from typing import Literal

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.workspace_access.groups import MigrationState
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport


class VerificationManager:
    def __init__(self, ws: WorkspaceClient, secrets_support: SecretScopesSupport):
        self._ws = ws
        self._secrets_support = secrets_support

    def verify(
        self, migration_state: MigrationState, target: Literal["backup", "account"], tuples: list[tuple[str, str]]
    ):
        for object_type, object_id in tuples:
            if object_type == "secrets":
                self.verify_applied_scope_acls(object_id, migration_state, target)
            else:
                self.verify_applied_permissions(object_type, object_id, migration_state, target)
        self.verify_roles_and_entitlements(migration_state, target)

    def verify_applied_permissions(
        self,
        object_type: str,
        object_id: str,
        migration_state: MigrationState,
        target: Literal["backup", "account"],
    ):
        base_attr = "temporary_name" if target == "backup" else "name_in_account"
        op = self._ws.permissions.get(object_type, object_id)
        for info in migration_state.groups:
            if not op.access_control_list:
                continue
            src_permissions = sorted(
                [
                    _
                    for _ in op.access_control_list
                    if _.group_name == info.name_in_workspace and _.group_name is not None
                ],
                key=lambda p: p.group_name,
            )
            dst_permissions = sorted(
                [
                    _
                    for _ in op.access_control_list
                    if _.group_name == getattr(info, base_attr) and _.group_name is not None
                ],
                key=lambda p: p.group_name,
            )
            assert len(dst_permissions) == len(
                src_permissions
            ), f"Target permissions were not applied correctly for {object_type}/{object_id}"
            assert [t.all_permissions for t in dst_permissions] == [
                s.all_permissions for s in src_permissions
            ], f"Target permissions were not applied correctly for {object_type}/{object_id}"

    def verify_applied_scope_acls(
        self, scope_name: str, migration_state: MigrationState, target: Literal["backup", "account"]
    ):
        base_attr = "name_in_workspace" if target == "backup" else "temporary_name"
        target_attr = "temporary_name" if target == "backup" else "name_in_account"
        for mi in migration_state.groups:
            src_name = getattr(mi, base_attr)
            dst_name = getattr(mi, target_attr)
            src_permission = self._secrets_support.secret_scope_permission(scope_name, src_name)
            dst_permission = self._secrets_support.secret_scope_permission(scope_name, dst_name)
            assert src_permission == dst_permission, "Scope ACLs were not applied correctly"

    def verify_roles_and_entitlements(self, migration_state: MigrationState, target: Literal["backup", "account"]):
        target_attr = "external_id" if target == "backup" else "id_in_workspace"
        for el in migration_state.groups:
            comparison_base = getattr(el, "id_in_workspace" if target == "backup" else "id_in_workspace")
            comparison_target = getattr(el, target_attr)

            base_group_info = self._ws.groups.get(comparison_base)
            target_group_info = self._ws.groups.get(comparison_target)

            assert base_group_info.roles == target_group_info.roles
            assert base_group_info.entitlements == target_group_info.entitlements
