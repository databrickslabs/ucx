import json
import logging
from collections.abc import Callable, Iterable
from datetime import timedelta
from functools import partial

from databricks.labs.blueprint.limiter import rate_limited
from databricks.sdk import WorkspaceClient
from databricks.sdk.retries import retried
from databricks.sdk.service import workspace
from databricks.sdk.service.workspace import AclItem

from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions, StaticListing
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


class SecretScopesSupport(AclSupport):
    def __init__(
        self,
        ws: WorkspaceClient,
        verify_timeout: timedelta | None = None,
        # this parameter is for testing scenarios only - [{object_type}:{object_id}]
        # it will use StaticListing class to return only object ids that has the same object type
        include_object_permissions: list[str] | None = None,
    ):
        self._ws = ws
        if verify_timeout is None:
            verify_timeout = timedelta(minutes=2)
        self._verify_timeout = verify_timeout
        self._include_object_permissions = include_object_permissions

    def get_crawler_tasks(self):
        def _crawler_task(scope: workspace.SecretScope):
            assert scope.name is not None
            acl_items = self._ws.secrets.list_acls(scope.name)
            return Permissions(
                object_id=scope.name,
                object_type="secrets",
                raw=json.dumps([item.as_dict() for item in acl_items]),
            )

        if self._include_object_permissions:
            for item in StaticListing(self._include_object_permissions, self.object_types()):
                yield partial(_crawler_task, workspace.SecretScope(name=item.object_id))
            return

        scopes = self._ws.secrets.list_scopes()
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
                self._applier_task(item.object_id, acl.principal, acl.permission)
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
        try:
            self._verify(scope_name, group_name, expected_permission)
        except ValueError:
            logger.info(f"Applying permissions again {expected_permission} to {group_name} for {scope_name}")
            self._ws.secrets.put_acl(scope_name, group_name, expected_permission)
            raise
        return True

    @rate_limited(max_requests=1100, burst_period_seconds=60)
    def _verify(self, scope_name: str, group_name: str, expected_permission: workspace.AclPermission):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        applied_permission = self.secret_scope_permission(scope_name, group_name)
        if applied_permission != expected_permission:
            msg = (
                f"Couldn't find permission for scope {scope_name} and group {group_name}\n"
                f"acl to be applied={expected_permission}\n"
                f"acl found in the object={applied_permission}\n"
            )
            raise ValueError(msg)
        return True

    def get_verify_task(self, item: Permissions) -> Callable[[], bool]:
        acls = [workspace.AclItem.from_dict(acl) for acl in json.loads(item.raw)]

        def _verify_acls(scope_name: str, acls: Iterable[AclItem]):
            for acl in acls:
                assert acl.permission is not None
                assert acl.principal is not None
                self._verify(scope_name, acl.principal, acl.permission)
            return True

        return partial(_verify_acls, item.object_id, acls)

    @rate_limited(max_requests=1100, burst_period_seconds=60)
    def _applier_task(self, object_id: str, principal: str, permission: workspace.AclPermission):
        self._ws.secrets.put_acl(object_id, principal, permission)
        retry_on_value_error = retried(on=[ValueError], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._reapply_on_failure)
        retried_check(object_id, principal, permission)
