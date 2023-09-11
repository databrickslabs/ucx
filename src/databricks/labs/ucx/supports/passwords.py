import json
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.inventory.types import (
    Destination,
    PermissionsInventoryItem,
    Supports,
)
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.supports.base import BaseSupport


class PasswordsSupport(BaseSupport):
    def __init__(self, ws: WorkspaceClient):
        super().__init__(ws, support_name=Supports.passwords)

    def get_crawler_tasks(self) -> list[Callable[..., PermissionsInventoryItem | None]]:
        def getter():
            permissions = self._ws.users.get_password_permissions()
            return PermissionsInventoryItem(
                object_id="passwords",
                support=self._support_name,
                raw_object_permissions=json.dumps(permissions.as_dict()),
            )

        return [getter]

    def is_item_relevant(self, _, __) -> bool:
        # passwords support is a workspace-level resource
        return True

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ):
        def setter():
            _permissions = iam.PasswordPermissions.from_dict(json.loads(item.raw_object_permissions))
            new_acl_requests: list[iam.PasswordAccessControlRequest] = []

            for acl_item in _permissions.access_control_list:
                if acl_item.group_name in [i.workspace.display_name for i in migration_state.groups]:
                    source_info = migration_state.get_by_workspace_group_name(acl_item.group_name)
                    target: iam.Group = getattr(source_info, destination)
                    for permission in acl_item.all_permissions:
                        # we don't want to set inherited permissions
                        if permission.inherited:
                            continue

                        _req = iam.PasswordAccessControlRequest(
                            group_name=target.display_name, permission_level=permission.permission_level
                        )
                        new_acl_requests.append(_req)
                else:
                    for permission in acl_item.all_permissions:
                        # we don't want to set inherited permissions
                        if permission.inherited:
                            continue

                        _req = iam.PasswordAccessControlRequest(
                            group_name=acl_item.group_name,
                            user_name=acl_item.user_name,
                            service_principal_name=acl_item.service_principal_name,
                            permission_level=permission.permission_level,
                        )
                        new_acl_requests.append(_req)

            self._ws.users.set_password_permissions(new_acl_requests)

        return setter
