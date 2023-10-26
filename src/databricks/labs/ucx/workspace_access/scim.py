import json
import logging
from datetime import timedelta
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group, Patch, PatchSchema

from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import (
    AclSupport,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.generic import RetryableError
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = logging.getLogger(__name__)


class ScimSupport(AclSupport):
    def __init__(self, ws: WorkspaceClient, verify_timeout: timedelta | None = timedelta(minutes=20)):
        self._ws = ws
        self._verify_timeout = verify_timeout

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: GroupMigrationState) -> bool:
        # TODO: This mean that we can lose entitlements, if we don't store the `$inventory.groups`
        return migration_state.is_id_in_scope(item.object_id)

    def get_crawler_tasks(self):
        groups = self._get_groups()
        with_roles = [g for g in groups if g.roles and len(g.roles) > 0]
        with_entitlements = [g for g in groups if g.entitlements and len(g.entitlements) > 0]
        for g in with_roles:
            yield partial(self._crawler_task, g, "roles")
        for g in with_entitlements:
            yield partial(self._crawler_task, g, "entitlements")

    # TODO remove after ES-892977 is fixed
    @retried(on=[DatabricksError])
    def _get_groups(self):
        return self._ws.groups.list(attributes="id,displayName,roles,entitlements")

    def object_types(self) -> set[str]:
        return {"roles", "entitlements"}

    def get_apply_task(self, item: Permissions, migration_state: GroupMigrationState, destination: Destination):
        if not self._is_item_relevant(item, migration_state):
            return None
        value = [iam.ComplexValue.from_dict(e) for e in json.loads(item.raw)]
        target_group_id = migration_state.get_target_id(item.object_id, destination)
        return partial(self._applier_task, group_id=target_group_id, value=value, property_name=item.object_type)

    @staticmethod
    def _crawler_task(group: iam.Group, property_name: str):
        return Permissions(
            object_id=group.id,
            object_type=property_name,
            raw=json.dumps([e.as_dict() for e in getattr(group, property_name)]),
        )

    def _inflight_check(self, group_id: str, value: list[iam.ComplexValue], property_name: str):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        set_retry_on_value_error = retried(on=[RetryableError], timeout=self._verify_timeout)
        set_retried_check = set_retry_on_value_error(self._safe_get_group)
        group = set_retried_check(group_id)
        if property_name == "roles" and group.roles:
            if all(elem in group.roles for elem in value):
                return True
        if property_name == "entitlements" and group.entitlements:
            if all(elem in group.entitlements for elem in value):
                return True
        msg = f"""Couldn't apply appropriate role for group {group_id}
                acl to be applied={[e.as_dict() for e in value]}
                acl found in the object={group.as_dict()}
                """
        raise ValueError(msg)

    @rate_limited(max_requests=10, burst_period_seconds=60)
    def _applier_task(self, group_id: str, value: list[iam.ComplexValue], property_name: str):
        operations = [iam.Patch(op=iam.PatchOp.ADD, path=property_name, value=[e.as_dict() for e in value])]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]

        patch_retry_on_value_error = retried(on=[RetryableError], timeout=self._verify_timeout)
        patch_retried_check = patch_retry_on_value_error(self._safe_patch_group)
        patch_retried_check(group_id=group_id, operations=operations, schemas=schemas)

        retry_on_value_error = retried(on=[ValueError], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._inflight_check)
        return retried_check(group_id, value, property_name)

    def _safe_patch_group(
        self, group_id: str, operations: list[Patch] | None = None, schemas: list[PatchSchema] | None = None
    ):
        try:
            return self._ws.groups.patch(id=group_id, operations=operations, schemas=schemas)
        except DatabricksError as e:
            if e.error_code in [
                "BAD_REQUEST",
                "INVALID_PARAMETER_VALUE",
                "UNAUTHORIZED",
                "PERMISSION_DENIED",
                "FEATURE_DISABLED",
                "RESOURCE_DOES_NOT_EXIST",
            ]:
                logger.warning(f"Could not apply changes to group {group_id} due to {e.error_code}")
                return None
            else:
                raise RetryableError() from e

    def _safe_get_group(self, group_id: str) -> Group | None:
        try:
            return self._ws.groups.get(group_id)
        except DatabricksError as e:
            if e.error_code in [
                "BAD_REQUEST",
                "INVALID_PARAMETER_VALUE",
                "UNAUTHORIZED",
                "PERMISSION_DENIED",
                "FEATURE_DISABLED",
                "RESOURCE_DOES_NOT_EXIST",
            ]:
                logger.warning(f"Could not get details of group {group_id} due to {e.error_code}")
                return None
            else:
                raise RetryableError() from e
