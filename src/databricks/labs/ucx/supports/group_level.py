import json
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.types import Destination, PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.supports.base import BaseSupport
from databricks.labs.ucx.utils import noop


class GroupLevelSupport(BaseSupport):
    def __init__(self, ws: WorkspaceClient, property_name: str):
        super().__init__(ws)
        self._ws = ws
        self._property_name = property_name

    def _crawler_task(self, group: iam.Group):
        return PermissionsInventoryItem(
            object_id=group.id,
            crawler=self._property_name,
            raw_object_permissions=json.dumps([e.as_dict() for e in getattr(group, self._property_name)]),
        )

    @sleep_and_retry
    @limits(calls=10, period=1)
    def _applier_task(self, group_id: str, value: list[iam.ComplexValue]):
        operations = [iam.Patch(op=iam.PatchOp.ADD, path=self._property_name, value=value)]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self._ws.groups.patch(group_id, operations=operations, schemas=schemas)

    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        return any(g.workspace.id == item.object_id for g in migration_state.groups)

    def get_crawler_tasks(self):
        groups = self._ws.groups.list(attributes=self._property_name)
        return [partial(self._crawler_task, g) if getattr(g, self._property_name) else partial(noop) for g in groups]

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
            return partial(self._applier_task, target_group_id, value)
