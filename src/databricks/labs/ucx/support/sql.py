import dataclasses
import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam, sql
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.types import Destination, PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.support.base import BaseSupport, logger


@dataclass
class SqlPermissionsInfo:
    object_id: str
    request_type: sql.ObjectTypePlural


class SqlPermissionsSupport(BaseSupport):
    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        mentioned_groups = [
            acl.group_name
            for acl in sql.GetResponse.from_dict(json.loads(item.raw_object_permissions)).access_control_list
        ]
        return any(g in mentioned_groups for g in [info.workspace.display_name for info in migration_state.groups])

    def __init__(
        self,
        ws: WorkspaceClient,
        listings: list[Callable[..., list[SqlPermissionsInfo]]],
    ):
        super().__init__(ws)
        self._listings = listings

    def _safe_get_dbsql_permissions(self, object_type: sql.ObjectTypePlural, object_id: str) -> sql.GetResponse | None:
        try:
            return self._ws.dbsql_permissions.get(object_type, object_id)
        except DatabricksError as e:
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise e

    @sleep_and_retry
    @limits(calls=100, period=1)
    def _crawler_task(self, object_id: str, object_type: sql.ObjectTypePlural) -> PermissionsInventoryItem | None:
        permissions = self._safe_get_dbsql_permissions(object_type=object_type, object_id=object_id)
        if permissions:
            return PermissionsInventoryItem(
                object_id=object_id,
                support=object_type.value,
                raw_object_permissions=json.dumps(permissions.as_dict()),
            )

    @sleep_and_retry
    @limits(calls=30, period=1)
    def _applier_task(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl]):
        """
        Please note that we only have SET option (DBSQL Permissions API doesn't support UPDATE operation).
        This affects the way how we prepare the new ACL request.
        """
        self._ws.dbsql_permissions.set(object_type=object_type, object_id=object_id, acl=acl)

    def get_crawler_tasks(self):
        for listing in self._listings:
            for item in listing():
                yield partial(self._crawler_task, item.object_id, item.request_type)

    def _prepare_new_acl(
        self, acl: list[sql.AccessControl], migration_state: GroupMigrationState, destination: Destination
    ) -> list[sql.AccessControl]:
        """
        Please note the comment above on how we apply these permissions.
        """
        acl_requests: list[sql.AccessControl] = []

        for acl_request in acl:
            if acl_request.group_name in [g.workspace.display_name for g in migration_state.groups]:
                migration_info = migration_state.get_by_workspace_group_name(acl_request.group_name)
                assert (
                    migration_info is not None
                ), f"Group {acl_request.group_name} is not in the migration groups provider"
                destination_group: iam.Group = getattr(migration_info, destination)
                new_acl_request = dataclasses.replace(acl_request, group_name=destination_group.display_name)
                acl_requests.append(new_acl_request)
            else:
                # no changes shall be applied
                acl_requests.append(acl_request)

        return acl_requests

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ):
        new_acl = self._prepare_new_acl(
            sql.GetResponse.from_dict(json.loads(item.raw_object_permissions)).access_control_list,
            migration_state,
            destination,
        )
        return partial(
            self._applier_task, object_type=sql.ObjectTypePlural(item.support), object_id=item.object_id, acl=new_acl
        )


def listing_wrapper(
    func: Callable[..., list], object_type: sql.ObjectTypePlural
) -> Callable[..., list[SqlPermissionsInfo]]:
    def wrapper() -> list[SqlPermissionsInfo]:
        for item in func():
            yield SqlPermissionsInfo(
                object_id=item.id,
                request_type=object_type,
            )

    return wrapper
