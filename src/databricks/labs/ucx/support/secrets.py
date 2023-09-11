import json
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, workspace
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.types import Destination, PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.support.base import BaseSupport


class SecretScopesSupport(BaseSupport):
    def __init__(self, ws: WorkspaceClient):
        super().__init__(ws=ws)

    def get_crawler_tasks(self):
        scopes = self._ws.secrets.list_scopes()

        def _crawler_task(scope: workspace.SecretScope):
            acl_items = self._ws.secrets.list_acls(scope.name)
            return PermissionsInventoryItem(
                object_id=scope.name,
                support="secrets",
                raw_object_permissions=json.dumps([item.as_dict() for item in acl_items]),
            )

        for scope in scopes:
            yield partial(_crawler_task, scope)

    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw_object_permissions)]
        mentioned_groups = [acl.principal for acl in acls]
        return any(g in mentioned_groups for g in [info.workspace.display_name for info in migration_state.groups])

    @sleep_and_retry
    @limits(calls=30, period=1)
    def _rate_limited_put_acl(self, object_id: str, principal: str, permission: workspace.AclPermission):
        self._ws.secrets.put_acl(object_id, principal, permission)

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw_object_permissions)]
        new_acls = []

        for acl in acls:
            if acl.principal in [i.workspace.display_name for i in migration_state.groups]:
                source_info = migration_state.get_by_workspace_group_name(acl.principal)
                target: iam.Group = getattr(source_info, destination)
                new_acls.append(workspace.AclItem(principal=target.display_name, permission=acl.permission))
            else:
                new_acls.append(acl)

        def apply_acls():
            for acl in new_acls:
                self._rate_limited_put_acl(item.object_id, acl.principal, acl.permission)

        return partial(apply_acls)
