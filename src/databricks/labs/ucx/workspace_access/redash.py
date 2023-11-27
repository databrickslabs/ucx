import dataclasses
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.retries import retried
from databricks.sdk.service import sql
from databricks.sdk.service.sql import ObjectTypePlural, SetResponse

from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.generic import RetryableError
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


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
    def __init__(
        self, ws: WorkspaceClient, listings: list[Listing], verify_timeout: timedelta | None = timedelta(minutes=1)
    ):
        self._ws = ws
        self._listings = listings
        self._verify_timeout = verify_timeout

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: MigrationState) -> bool:
        mentioned_groups = [
            acl.group_name for acl in sql.GetResponse.from_dict(json.loads(item.raw)).access_control_list
        ]
        return any(g in mentioned_groups for g in [info.name_in_workspace for info in migration_state.groups])

    def get_crawler_tasks(self):
        for listing in self._listings:
            for item in listing:
                yield partial(self._crawler_task, item.object_id, item.request_type)

    def object_types(self) -> set[str]:
        all_object_types = set()
        for listing in self._listings:
            all_object_types.add(listing.object_type)
        return all_object_types

    def get_apply_task(self, item: Permissions, migration_state: MigrationState):
        if not self._is_item_relevant(item, migration_state):
            return None
        new_acl = self._prepare_new_acl(
            sql.GetResponse.from_dict(json.loads(item.raw)).access_control_list, migration_state
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
            logger.error(f"DatabricksError {e}  error_code: {e.error_code}")
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {object_type} {object_id} due to {e.error_code}")
                return None
            else:
                msg = f"{e.error_code} can be retried, doing another attempt..."
                raise RetryableError(message=msg) from e

    @rate_limited(max_requests=100)
    def _crawler_task(self, object_id: str, object_type: sql.ObjectTypePlural) -> Permissions | None:
        permissions = self._safe_get_dbsql_permissions(object_type=object_type, object_id=object_id)
        if permissions:
            return Permissions(
                object_id=object_id,
                object_type=object_type.value,
                raw=json.dumps(permissions.as_dict()),
            )

    def _inflight_check(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl]):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        remote_permission = self._safe_get_dbsql_permissions(object_type, object_id)
        if remote_permission:
            if all(elem in remote_permission.access_control_list for elem in acl):
                return True
            else:
                msg = f"""
                Couldn't apply appropriate permission for object type {object_type} with id {object_id}
                acl to be applied={acl}
                acl found in the object={remote_permission}
                """
                raise ValueError(msg)
        return False

    @rate_limited(max_requests=30)
    def _applier_task(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl]):
        """
        Please note that we only have SET option (DBSQL Permissions API doesn't support UPDATE operation).
        This affects the way how we prepare the new ACL request.
        """

        set_retry_on_value_error = retried(on=[RetryableError], timeout=self._verify_timeout)
        set_retried_check = set_retry_on_value_error(self._safe_set_permissions)
        set_retried_check(object_type, object_id, acl)

        retry_on_value_error = retried(on=[ValueError, RetryableError], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._inflight_check)
        return retried_check(object_type, object_id, acl)

    def _prepare_new_acl(
        self, acl: list[sql.AccessControl], migration_state: MigrationState
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
            target_principal = migration_state.get_target_principal(access_control.group_name)
            if target_principal is None:
                logger.debug(f"Skipping redash item for `{access_control.group_name}`: no target principal")
                acl_requests.append(access_control)
                continue
            new_acl_request = dataclasses.replace(access_control, group_name=target_principal)
            acl_requests.append(new_acl_request)
        return acl_requests

    @retried(on=[RetryableError], timeout=timedelta(minutes=12))
    @rate_limited(burst_period_seconds=30)
    def _safe_set_permissions(
        self, object_type: ObjectTypePlural, object_id: str, acl: list[sql.AccessControl] | None
    ) -> SetResponse | None:
        def hash_permissions(permissions: list[sql.AccessControl]):
            return {
                hash((permission.permission_level, permission.user_name, permission.group_name))
                for permission in permissions
            }

        try:
            res = self._ws.dbsql_permissions.set(object_type=object_type, object_id=object_id, access_control_list=acl)
            if hash_permissions(acl).issubset(hash_permissions(res.access_control_list)):
                return res
            else:
                msg = (
                    f"Failed to set permission and will be retried for {object_type} {object_id}, "
                    f"doing another attempt..."
                )
                raise RetryableError(message=msg)
        except DatabricksError as e:
            logger.error(f"DatabricksError {e}  error_code: {e.error_code}")
            if e.error_code in [
                "BAD_REQUEST",
                "UNAUTHORIZED",
                "PERMISSION_DENIED",
                "NOT_FOUND",
            ]:
                logger.warning(f"Could not update permissions for {object_type} {object_id} due to {e.error_code}")
                return None
            else:
                msg = f"{e.error_code} can be retried for {object_type} {object_id}, doing another attempt..."
                raise RetryableError(message=msg) from e


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
