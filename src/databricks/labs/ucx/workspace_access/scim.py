import json
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import (
    Applier,
    Crawler,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState


class ScimSupport(Crawler, Applier):
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def is_item_relevant(self, item: Permissions, migration_state: GroupMigrationState) -> bool:
        return any(g.workspace.id == item.object_id for g in migration_state.groups)

    def get_crawler_tasks(self):
        groups = self._ws.groups.list(attributes="id,displayName,roles,entitlements")
        with_roles = [g for g in groups if g.roles and len(g.roles) > 0]
        with_entitlements = [g for g in groups if g.entitlements and len(g.entitlements) > 0]
        for g in with_roles:
            yield partial(self._crawler_task, g, "roles")
        for g in with_entitlements:
            yield partial(self._crawler_task, g, "entitlements")

    def _get_apply_task(
        self, item: Permissions, migration_state: GroupMigrationState, destination: Destination
    ):
        value = [iam.ComplexValue.from_dict(e) for e in json.loads(item.raw_object_permissions)]
        target_info = [g for g in migration_state.groups if g.workspace.id == item.object_id]
        if len(target_info) == 0:
            msg = f"Could not find group with ID {item.object_id}"
            raise ValueError(msg)
        else:
            target_group_id = getattr(target_info[0], destination).id
            return partial(self._applier_task, group_id=target_group_id, value=value, property_name=item.object_type)

    def _crawler_task(self, group: iam.Group, property_name: str):
        return Permissions(
            object_id=group.id,
            support=property_name,
            raw_object_permissions=json.dumps([e.as_dict() for e in getattr(group, property_name)]),
        )

    @rate_limited(max_requests=10)
    def _applier_task(self, group_id: str, value: list[iam.ComplexValue], property_name: str):
        operations = [iam.Patch(op=iam.PatchOp.ADD, path=property_name, value=[e.as_dict() for e in value])]
        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self._ws.groups.patch(id=group_id, operations=operations, schemas=schemas)
