import json
import logging
from datetime import timedelta
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.retries import retried
from databricks.sdk.service import workspace

from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


class SecretScopesSupport(AclSupport):
    def __init__(self, ws: WorkspaceClient, verify_timeout: timedelta | None = None):
        self._ws = ws
        if verify_timeout is None:
            verify_timeout = timedelta(minutes=2)
        self._verify_timeout = verify_timeout

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

    def get_apply_task(self, item: Permissions, migration_state: MigrationState):
        if not self._is_item_relevant(item, migration_state):
            return None

        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw)]
        new_acls = []

        for acl in acls:
            if not migration_state.is_in_scope(acl.principal):
                new_acls.append(acl)
                continue
            target_principal = migration_state.get_target_principal(acl.principal)
            if target_principal is None:
                logger.debug(f"Skipping {acl.principal} because of no target principal")
                continue
            new_acls.append(workspace.AclItem(principal=target_principal, permission=acl.permission))

        def apply_acls():
            for acl in new_acls:
                self._rate_limited_put_acl(item.object_id, acl.principal, acl.permission)
            return True

        return partial(apply_acls)

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: MigrationState) -> bool:
        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw)]
        mentioned_groups = [acl.principal for acl in acls]
        return any(g in mentioned_groups for g in [info.name_in_workspace for info in migration_state.groups])

    def secret_scope_permission(self, scope_name: str, group_name: str) -> workspace.AclPermission | None:
        for acl in self._ws.secrets.list_acls(scope=scope_name):
            if acl.principal == group_name:
                return acl.permission
        return None

    def _reapply_on_failure(self, scope_name: str, group_name: str, expected_permission: workspace.AclPermission):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        applied_permission = self.secret_scope_permission(scope_name, group_name)
        if applied_permission != expected_permission:
            # try to apply again if the permissions are not equal: sometimes the list_acls api is inconsistent
            logger.info(f"Applying permissions again {expected_permission} to {group_name} for {scope_name}")
            self._ws.secrets.put_acl(scope_name, group_name, expected_permission)
            msg = (
                f"Applied permission {applied_permission} is not equal to expected "
                f"permission {expected_permission} for {scope_name} and {group_name}!"
            )
            raise ValueError(msg)
        logger.info(f"Permissions matched for {scope_name}, {group_name} and {expected_permission}!")
        return True

    @rate_limited(max_requests=1100, burst_period_seconds=60)
    def _rate_limited_put_acl(self, object_id: str, principal: str, permission: workspace.AclPermission):
        self._ws.secrets.put_acl(object_id, principal, permission)
        retry_on_value_error = retried(on=[ValueError], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._reapply_on_failure)
        retried_check(object_id, principal, permission)
