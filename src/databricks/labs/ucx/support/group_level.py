import json
from functools import partial

from databricks.sdk.service import iam
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.types import Destination, PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.support.base import BaseSupport


class ScimSupport(BaseSupport):
    def _crawler_task(self, group: iam.Group, property_name: str):
        return PermissionsInventoryItem(
            object_id=group.id,
            support=property_name,
            raw_object_permissions=json.dumps([e.as_dict() for e in getattr(group, property_name)]),
        )

    @sleep_and_retry
    @limits(calls=10, period=1)
    def _applier_task(self, group_id: str, value: list[iam.ComplexValue], property_name: str):
        operations = [iam.Patch(op=iam.PatchOp.ADD, path=property_name, value=value)]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self._ws.groups.patch(group_id, operations=operations, schemas=schemas)

    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        return any(g.workspace.id == item.object_id for g in migration_state.groups)

    def get_crawler_tasks(self):
        groups = self._ws.groups.list(attributes="id,displayName,roles,entitlements")
        with_roles = [g for g in groups if len(g.roles) > 0]
        with_entitlements = [g for g in groups if len(g.entitlements) > 0]
        for g in with_roles:
            yield partial(self._crawler_task, g)
        for g in with_entitlements:
            yield partial(self._crawler_task, g)

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ):
        value = [iam.ComplexValue.from_dict(e) for e in json.loads(item.raw_object_permissions)]
        target_info = [g for g in migration_state.groups if g.workspace.id == item.object_id]
        if len(target_info) == 0:
            msg = f"Could not find group with ID {item.object_id}"
            raise ValueError(msg)
        else:
            target_group_id = getattr(target_info[0], destination).id
            return partial(self._applier_task, group_id=target_group_id, value=value, property_name=item.support)
