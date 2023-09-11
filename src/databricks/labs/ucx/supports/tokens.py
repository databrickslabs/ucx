import json
from collections.abc import Callable

from databricks.sdk.service import iam, settings

from databricks.labs.ucx.inventory.types import Destination, PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.supports.base import BaseSupport


class TokensSupport(BaseSupport):
    def get_crawler_tasks(self) -> list[Callable[..., PermissionsInventoryItem | None]]:
        def token_getter() -> PermissionsInventoryItem:
            return PermissionsInventoryItem(
                object_id="tokens",
                support="tokens",
                raw_object_permissions=json.dumps(self._ws.token_management.get_token_permissions().as_dict()),
            )

        return [token_getter]

    def is_item_relevant(self, _, __) -> bool:
        # token settings exist only on the whole workspace level, the relevance check is noop
        return True

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ):
        def apply_tokens():
            permissions = settings.TokenPermissions.from_dict(json.loads(item.raw_object_permissions))
            new_acl_requests: list[settings.TokenAccessControlRequest] = []

            for acl_item in permissions.access_control_list:
                if acl_item.group_name in [i.workspace for i in migration_state.groups]:
                    source_info = migration_state.get_by_workspace_group_name(acl_item.group_name)
                    target: iam.Group = getattr(source_info, destination)
                    for permission in acl_item.all_permissions:
                        _req = settings.TokenAccessControlRequest(
                            group_name=target.display_name, permission_level=permission
                        )
                        new_acl_requests.append(_req)
                else:
                    for permission in acl_item.all_permissions:
                        _req = settings.TokenAccessControlRequest(
                            group_name=acl_item.group_name,
                            user_name=acl_item.user_name,
                            service_principal_name=acl_item.service_principal_name,
                            permission_level=permission,
                        )
                        new_acl_requests.append(_req)

            self._ws.token_management.set_token_permissions(new_acl_requests)

        return apply_tokens
