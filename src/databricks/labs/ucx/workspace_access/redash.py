import dataclasses
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import sql

from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import (
    AclSupport,
    Destination,
    Permissions,
    logger,
)
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState


@dataclass
class SqlPermissionsInfo:
    object_id: str
    request_type: sql.ObjectTypePlural


# This module is called redash to disambiguate from databricks.sdk.service.sql


class Listing:
    def __init__(self, func: Callable[..., list], request_type: sql.ObjectTypePlural):
        self._func = func
        self._request_type = request_type
        self.object_type = request_type.value

    def __iter__(self):
        for item in self._func():
            yield SqlPermissionsInfo(item.id, self._request_type)


class RedashPermissionsSupport(AclSupport):
    def __init__(self, ws: WorkspaceClient, listings: list[Listing]):
        self._ws = ws
        self._listings = listings

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: GroupMigrationState) -> bool:
        object_permissions = sql.GetResponse.from_dict(json.loads(item.raw))
        for acl in object_permissions.access_control_list:
            if not migration_state.is_in_scope(acl.group_name):
                continue
            return True
        return False

    def get_crawler_tasks(self):
        for listing in self._listings:
            for item in listing:
                yield partial(self._crawler_task, item.object_id, item.request_type)

    def object_types(self) -> set[str]:
        all_object_types = set()
        for listing in self._listings:
            all_object_types.add(listing.object_type)
        return all_object_types

    def get_apply_task(self, item: Permissions, migration_state: GroupMigrationState, destination: Destination):
        if not self._is_item_relevant(item, migration_state):
            return None
        new_acl = self._prepare_new_acl(
            sql.GetResponse.from_dict(json.loads(item.raw)).access_control_list,
            migration_state,
            destination,
        )
        return partial(
            self._applier_task,
            object_type=sql.ObjectTypePlural(item.object_type),
            object_id=item.object_id,
            acl=new_acl,
        )

    def _safe_get_dbsql_permissions(self, object_type: sql.ObjectTypePlural, object_id: str) -> sql.GetResponse | None:
        try:
            return self._ws.dbsql_permissions.get(object_type, object_id)
        except DatabricksError as e:
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise e

    @rate_limited(max_requests=100)
    def _crawler_task(self, object_id: str, object_type: sql.ObjectTypePlural) -> Permissions | None:
        permissions = self._safe_get_dbsql_permissions(object_type=object_type, object_id=object_id)
        if permissions:
            return Permissions(
                object_id=object_id,
                object_type=object_type.value,
                raw=json.dumps(permissions.as_dict()),
            )

    @rate_limited(max_requests=30)
    def _applier_task(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl]):
        """
        Please note that we only have SET option (DBSQL Permissions API doesn't support UPDATE operation).
        This affects the way how we prepare the new ACL request.
        """
        for _i in range(0, 3):
            self._ws.dbsql_permissions.set(object_type=object_type, object_id=object_id, access_control_list=acl)

            remote_permission = self._safe_get_dbsql_permissions(object_type, object_id)
            if all(elem in remote_permission.access_control_list for elem in acl):
                return True

            logger.warning(
                f"""Couldn't apply appropriate permission for object type {object_type} with id {object_id}
            acl to be applied={acl}
            acl found in the object={remote_permission}
            """
            )
            time.sleep(1 + _i)
        return False

    def _prepare_new_acl(
        self, acl: list[sql.AccessControl], migration_state: GroupMigrationState, destination: Destination
    ) -> list[sql.AccessControl]:
        """
        Please note the comment above on how we apply these permissions.
        """
        acl_requests: list[sql.AccessControl] = []
        for access_control in acl:
            if not migration_state.is_in_scope(access_control.group_name):
                logger.debug(f"Skipping redash item for `{access_control.group_name}`: not in scope")
                acl_requests.append(access_control)
                continue
            target_principal = migration_state.get_target_principal(access_control.group_name, destination)
            if target_principal is None:
                logger.debug(f"Skipping redash item for `{access_control.group_name}`: no target principal")
                acl_requests.append(access_control)
                continue
            new_acl_request = dataclasses.replace(access_control, group_name=target_principal)
            acl_requests.append(new_acl_request)
        return acl_requests


def redash_listing_wrapper(
    func: Callable[..., list], object_type: sql.ObjectTypePlural
) -> Callable[..., list[SqlPermissionsInfo]]:
    def wrapper() -> list[SqlPermissionsInfo]:
        for item in func():
            yield SqlPermissionsInfo(
                object_id=item.id,
                request_type=object_type,
            )

    return wrapper
