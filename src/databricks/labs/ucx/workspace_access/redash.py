# pylint: disable=duplicate-code
import dataclasses
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import timedelta
from functools import partial

from databricks.labs.blueprint.limiter import rate_limited
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, NotFound, PermissionDenied
from databricks.sdk.retries import retried
from databricks.sdk.service import sql
from databricks.sdk.service.sql import ObjectTypePlural, SetResponse

from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions, StaticListing
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


@dataclass
class SqlPermissionsInfo:
    object_id: str
    request_type: sql.ObjectTypePlural


# This module is called redash to disambiguate from databricks.sdk.service.sql


class Listing:
    def __init__(self, func: Callable[..., Iterable], request_type: sql.ObjectTypePlural):
        self._func = func
        self._request_type = request_type
        self.object_type = request_type.value

    def __iter__(self):
        for item in self._func():
            yield SqlPermissionsInfo(item.id, self._request_type)

    def __repr__(self):
        return f"Listing({self.object_type} via {self._func.__qualname__})"


class RedashPermissionsSupport(AclSupport):
    def __init__(
        self,
        ws: WorkspaceClient,
        listings: list[Listing],
        set_permissions_timeout: timedelta | None = timedelta(minutes=1),
        # Group information in Redash are cached for up to 10 minutes causing inconsistencies.
        # If a group is renamed, the old name may still be returned by the dbsql get permissions api.
        # Note that the update/set API is strongly consistent and is not affected by this behaviour.
        # The validation step should keep retrying for at least 10 mins until the get api returns the new group name.
        # More details here: https://databricks.atlassian.net/browse/ES-992619
        verify_timeout: timedelta | None = timedelta(minutes=11),
        # this parameter is for testing scenarios only - [{object_type}:{object_id}]
        # it will use StaticListing class to return only object ids that has the same object type
        include_object_permissions: list[str] | None = None,
    ):
        self._ws = ws
        self._listings = listings
        self._set_permissions_timeout = set_permissions_timeout
        self._verify_timeout = verify_timeout
        self._include_object_permissions = include_object_permissions

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: MigrationState) -> bool:
        permissions_response = sql.GetResponse.from_dict(json.loads(item.raw))
        assert permissions_response.access_control_list is not None
        mentioned_groups = [acl.group_name for acl in permissions_response.access_control_list]
        return any(g in mentioned_groups for g in [info.name_in_workspace for info in migration_state.groups])

    def get_crawler_tasks(self):
        if self._include_object_permissions:
            for item in StaticListing(self._include_object_permissions, self.object_types()):
                yield partial(self._crawler_task, item.object_id, sql.ObjectTypePlural(item.object_type))
            return
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
        permissions = sql.GetResponse.from_dict(json.loads(item.raw))
        assert permissions.access_control_list is not None
        new_acl = self._prepare_new_acl(permissions.access_control_list, migration_state)
        return partial(
            self._applier_task,
            object_type=sql.ObjectTypePlural(item.object_type),
            object_id=item.object_id,
            acl=new_acl,
        )

    def _safe_get_dbsql_permissions(self, object_type: sql.ObjectTypePlural, object_id: str) -> sql.GetResponse | None:
        try:
            return self._ws.dbsql_permissions.get(object_type, object_id)
        except NotFound:
            logger.warning(f"removed on backend: {object_type} {object_id}")
            return None

    def _load_as_request(self, object_type: sql.ObjectTypePlural, object_id: str) -> list[sql.AccessControl]:
        loaded = self._safe_get_dbsql_permissions(object_type, object_id)
        if loaded is None:
            return []
        acl: list[sql.AccessControl] = []
        if not loaded.access_control_list:
            return acl

        for access_control in loaded.access_control_list:
            acl.append(
                sql.AccessControl(
                    permission_level=access_control.permission_level,
                    group_name=access_control.group_name,
                    user_name=access_control.user_name,
                )
            )
        # sort to return deterministic results
        return sorted(acl, key=lambda v: f"{v.group_name}:{v.user_name}")

    def load_as_dict(self, object_type: sql.ObjectTypePlural, object_id: str) -> dict[str, sql.PermissionLevel]:
        result = {}
        for acl in self._load_as_request(object_type, object_id):
            if not acl.permission_level:
                continue
            result[self._key_for_acl_dict(acl)] = acl.permission_level
        return result

    @staticmethod
    def _key_for_acl_dict(acl: sql.AccessControl) -> str:
        if acl.group_name is not None:
            return acl.group_name
        if acl.user_name is not None:
            return acl.user_name
        return "UNKNOWN"

    @rate_limited(max_requests=100)
    def _crawler_task(self, object_id: str, object_type: sql.ObjectTypePlural) -> Permissions | None:
        permissions = self._safe_get_dbsql_permissions(object_type=object_type, object_id=object_id)
        if permissions:
            return Permissions(
                object_id=object_id,
                object_type=object_type.value,
                raw=json.dumps(permissions.as_dict()),
            )
        return None

    @rate_limited(max_requests=100)
    def _verify(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl]):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        remote_permission = self._safe_get_dbsql_permissions(object_type, object_id)
        if remote_permission:
            assert remote_permission.access_control_list is not None
            if all(elem in remote_permission.access_control_list for elem in acl):
                return True
            msg = (
                f"Couldn't find permission for object type {object_type} with id {object_id}\n"
                f"acl to be applied={acl}\n"
                f"acl found in the object={remote_permission}\n"
            )
            raise NotFound(msg)
        return False

    def get_verify_task(self, item: Permissions) -> Callable[[], bool]:
        acl = sql.GetResponse.from_dict(json.loads(item.raw))
        if not acl.access_control_list:
            raise ValueError(
                f"Access control list not present for object type " f"{item.object_type} and object id {item.object_id}"
            )
        return partial(self._verify, sql.ObjectTypePlural(item.object_type), item.object_id, acl.access_control_list)

    @rate_limited(max_requests=30)
    def _applier_task(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl]):
        """
        Please note that we only have SET option (DBSQL Permissions API doesn't support UPDATE operation).
        This affects the way how we prepare the new ACL request.
        """

        set_retry_on_value_error = retried(on=[InternalError, ValueError], timeout=self._set_permissions_timeout)
        set_retried_check = set_retry_on_value_error(self._safe_set_permissions)
        set_retried_check(object_type, object_id, acl)

        retry_on_value_error = retried(on=[InternalError, NotFound], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._verify)
        return retried_check(object_type, object_id, acl)

    @staticmethod
    def _prepare_new_acl(acl: list[sql.AccessControl], migration_state: MigrationState) -> list[sql.AccessControl]:
        """
        Please note the comment above on how we apply these permissions.
        Permissions are set/replaced and not updated/patched, therefore all existing ACLs need to be collected
        including users and temp/backup groups.
        """
        acl_requests: list[sql.AccessControl] = []
        for access_control in acl:
            if access_control.user_name:
                logger.debug(f"Including redash permissions acl for user: `{access_control.user_name}`")
                acl_requests.append(access_control)
                continue

            if not access_control.group_name:
                continue

            if not migration_state.is_in_scope(access_control.group_name):
                logger.debug(
                    f"Including redash permissions acl for group not in the scope: `{access_control.group_name}`"
                )
                acl_requests.append(access_control)
                continue

            target_principal = migration_state.get_target_principal(access_control.group_name)
            if not target_principal:
                logger.debug(
                    f"Including redash permissions acl for group without target principal: "
                    f"`{access_control.group_name}`"
                )
                acl_requests.append(access_control)
                continue

            logger.debug(f"Including redash permissions acl for target group `{target_principal}`")
            new_acl_request = dataclasses.replace(access_control, group_name=target_principal)
            acl_requests.append(new_acl_request)

            temp_principal = migration_state.get_temp_principal(access_control.group_name)
            if temp_principal is not None:
                logger.debug(f"Including redash permissions acl for temp group `{temp_principal}`")
                temp_group_new_acl_request = dataclasses.replace(access_control, group_name=temp_principal)
                acl_requests.append(temp_group_new_acl_request)

        return acl_requests

    @rate_limited(burst_period_seconds=30)
    def _safe_set_permissions(
        self, object_type: ObjectTypePlural, object_id: str, acl: list[sql.AccessControl] | None
    ) -> SetResponse | None:
        assert acl is not None

        def hash_permissions(permissions: list[sql.AccessControl]):
            return {
                hash((permission.permission_level, permission.user_name, permission.group_name))
                for permission in permissions
            }

        try:
            res = self._ws.dbsql_permissions.set(object_type=object_type, object_id=object_id, access_control_list=acl)
            assert res.access_control_list is not None
            if hash_permissions(acl).issubset(hash_permissions(res.access_control_list)):
                return res
            msg = (
                f"Failed to set permission and will be retried for {object_type} {object_id}, "
                f"doing another attempt..."
            )
            raise ValueError(msg)
        except PermissionDenied:
            logger.warning(f"Permission denied: {object_type} {object_id}")
            return None
        except NotFound:
            logger.warning(f"Deleted on platform: {object_type} {object_id}")
            return None

    def __repr__(self):
        return f"RedashPermissionsSupport({self._listings})"


def redash_listing_wrapper(
    func: Callable[..., list], object_type: sql.ObjectTypePlural
) -> Callable[..., Iterable[SqlPermissionsInfo]]:
    def wrapper() -> Iterable[SqlPermissionsInfo]:
        for item in func():
            yield SqlPermissionsInfo(
                object_id=item.id,
                request_type=object_type,
            )

    return wrapper
