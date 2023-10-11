import json
import random
import time
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, workspace

from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import (
    AclSupport,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState


class SecretScopesSupport(AclSupport):
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def get_crawler_tasks(self):
        scopes = self._ws.secrets.list_scopes()

        def _crawler_task(scope: workspace.SecretScope):
            acl_items = self._ws.secrets.list_acls(scope.name)
            return Permissions(
                object_id=scope.name,
                object_type="secrets",
                raw=json.dumps([item.as_dict() for item in acl_items]),
            )

        for scope in scopes:
            yield partial(_crawler_task, scope)

    def object_types(self) -> set[str]:
        return {"secrets"}

    def get_apply_task(self, item: Permissions, migration_state: GroupMigrationState, destination: Destination):
        if not self._is_item_relevant(item, migration_state):
            return None

        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw)]
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
            return True

        return partial(apply_acls)

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: GroupMigrationState) -> bool:
        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw)]
        mentioned_groups = [acl.principal for acl in acls]
        return any(g in mentioned_groups for g in [info.workspace.display_name for info in migration_state.groups])

    def secret_scope_permission(self, scope_name: str, group_name: str) -> workspace.AclPermission | None:
        for acl in self._ws.secrets.list_acls(scope=scope_name):
            if acl.principal == group_name:
                return acl.permission
        return None

    def _inflight_check(
        self, scope_name: str, group_name: str, expected_permission: workspace.AclPermission, num_retries: int = 5
    ):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        # TODO: add mixin to SDK
        retries_left = num_retries
        while retries_left > 0:
            time.sleep(random.random() * 2)
            applied_permission = self.secret_scope_permission(scope_name=scope_name, group_name=group_name)
            if applied_permission:
                if applied_permission == expected_permission:
                    return
                else:
                    msg = (
                        f"Applied permission {applied_permission} is not "
                        f"equal to expected permission {expected_permission}"
                    )
                    raise ValueError(msg)

            retries_left -= 1

        msg = f"Failed to apply permissions for {group_name} on scope {scope_name} in {num_retries} retries"
        raise ValueError(msg)

    @rate_limited(max_requests=30)
    def _rate_limited_put_acl(self, object_id: str, principal: str, permission: workspace.AclPermission):
        self._ws.secrets.put_acl(object_id, principal, permission)
        self._inflight_check(scope_name=object_id, group_name=principal, expected_permission=permission)
