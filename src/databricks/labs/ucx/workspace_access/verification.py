import logging
from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist

from databricks.labs.ucx.workspace_access.groups import MigrationState
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport

logger = logging.getLogger(__name__)


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
        try:
            permission = self._ws.permissions.get(object_type, object_id)
        except ResourceDoesNotExist:
            logger.warning(f"removed on backend: {object_type}.{object_id}")
            return
        for info in migration_state.groups:
            if not permission.access_control_list:
                continue
            src_permissions = sorted(
                [
                    _
                    for _ in permission.access_control_list
                    if _.group_name == info.name_in_workspace and _.group_name is not None
                ],
                key=lambda p: p.group_name,
            )
            dst_permissions = sorted(
                [
                    _
                    for _ in permission.access_control_list
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
        for migrated_group in migration_state.groups:
            src_name = getattr(migrated_group, base_attr)
            dst_name = getattr(migrated_group, target_attr)
            src_permission = self._secrets_support.secret_scope_permission(scope_name, src_name)
            dst_permission = self._secrets_support.secret_scope_permission(scope_name, dst_name)
            assert src_permission == dst_permission, "Scope ACLs were not applied correctly"

    def verify_roles_and_entitlements(self, migration_state: MigrationState, target: Literal["backup", "account"]):
        target_attr = "external_id" if target == "backup" else "id_in_workspace"
        for migrated_group in migration_state.groups:
            comparison_base = getattr(migrated_group, "id_in_workspace" if target == "backup" else "id_in_workspace")
            comparison_target = getattr(migrated_group, target_attr)

            try:
                base_group_info = self._ws.groups.get(comparison_base)
            except NotFound:
                logger.warning(f"removed on backend: {comparison_base}")
                continue
            try:
                target_group_info = self._ws.groups.get(comparison_target)
            except NotFound:
                logger.warning(f"removed on backend: {comparison_target}")
                continue

            assert base_group_info.roles == target_group_info.roles
            assert base_group_info.entitlements == target_group_info.entitlements


class VerifyHasMetastore:
    def __init__(self, ws: WorkspaceClient):
        self.metastore_id: str | None = None
        self.default_catalog_name: str | None = None
        self.workspace_id: int | None = None
        self._ws = ws

    def verify_metastore(self):
        """
        Verifies if a metastore exists for a metastore
        :param :
        :return:
        """

        try:
            current_metastore = self._ws.metastores.current()
            if current_metastore:
                self.default_catalog_name = current_metastore.default_catalog_name
                self.metastore_id = current_metastore.metastore_id
                self.workspace_id = current_metastore.workspace_id
                return True
            raise MetastoreNotFoundError
        except PermissionDenied:
            logger.error("Permission Denied while trying to access metastore")
            return False


class MetastoreNotFoundError(Exception):
    def __init__(self, message="Metastore not found in the workspace"):
        self.message = message
        super().__init__(self.message)
