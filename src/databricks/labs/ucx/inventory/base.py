import json
from abc import ABC, abstractmethod
from collections.abc import Callable
from logging import Logger
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import sql
from databricks.sdk.service.iam import (
    AccessControlRequest,
    ComplexValue,
    Group,
    ObjectPermissions,
    Patch,
    PatchOp,
    PatchSchema,
)
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.types import (
    Destination,
    PermissionsInventoryItem,
    RequestObjectType,
)
from databricks.labs.ucx.providers.groups_info import GroupMigrationState

logger = Logger(__name__)


class ClientMixin:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws


class BaseTask:
    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


class CrawlerTask(BaseTask):
    @abstractmethod
    def __call__(self, *args, **kwargs) -> PermissionsInventoryItem | None:
        pass


class NoopTask(CrawlerTask):
    def __call__(self, *args, **kwargs):
        pass


class ApplierTask(BaseTask):
    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


class CrawlerMixin:
    @abstractmethod
    def get_crawler_tasks(self) -> list[CrawlerTask]:
        pass


class ApplierMixin:
    @abstractmethod
    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        pass

    @abstractmethod
    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> ApplierTask:
        """
        This method should return an instance of ApplierTask.
        """

    def get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> ApplierTask | NoopTask:
        if self.is_item_relevant(item, migration_state):
            return self._get_apply_task(item, migration_state, destination)
        else:
            return NoopTask()


class SerdeMixin:
    @classmethod
    @abstractmethod
    def _serialize(cls, typed: Any) -> str:
        pass

    @classmethod
    @abstractmethod
    def _deserialize(cls, raw: str) -> Any:
        pass


class BaseSupport(ABC, ClientMixin, CrawlerMixin, ApplierMixin, SerdeMixin):
    """
    Base class for all support classes.
    Child classes must implement all abstract methods.
    """


class GroupLevelSupport(BaseSupport):
    """
    Base class for group-level properties.
    """

    property_name: str

    class GroupLevelCrawlerTask(CrawlerTask):
        def __init__(self, group: Group, property_name: str):
            self._group = group
            self._property_name = property_name

        def __call__(self, _, __) -> PermissionsInventoryItem:
            return PermissionsInventoryItem(
                object_id=self._group.id,
                crawler=self._property_name,
                raw_object_permissions=GroupLevelSupport._serialize(self._group),
            )

    class GroupLevelApplierTask(ApplierTask):
        def __init__(self, ws: WorkspaceClient, group_id: str, property_name: str, value: list[ComplexValue]):
            self._group_id = group_id
            self._value = value
            self._ws = ws
            self._property_name = property_name

        @sleep_and_retry
        @limits(calls=10, period=1)
        def __call__(self, _, __):
            operations = [Patch(op=PatchOp.ADD, path=self._property_name, value=self._value)]
            schemas = [PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
            self._ws.groups.patch(self._group_id, operations=operations, schemas=schemas)

    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        return any(g.workspace.id == item.object_id for g in migration_state.groups)

    @classmethod
    def _serialize(cls, group: Group) -> str:
        return json.dumps([e.as_dict() for e in getattr(group, cls.property_name)])

    @classmethod
    def _deserialize(cls, raw: str) -> list[ComplexValue]:
        return [ComplexValue.from_dict(e) for e in json.loads(raw)]

    def get_crawler_tasks(self) -> list[CrawlerTask]:
        groups = self._ws.groups.list(attributes=self.property_name)
        return [
            self.GroupLevelCrawlerTask(g, self.property_name) if getattr(g, self.property_name) else NoopTask()
            for g in groups
        ]

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> ApplierTask:
        value = self._deserialize(item.raw_object_permissions)
        target_info = [g for g in migration_state.groups if g.workspace.id == item.object_id]
        if len(target_info) == 0:
            msg = f"Could not find group with ID {item.object_id}"
            raise ValueError(msg)
        else:
            target_group_id = getattr(target_info[0], destination).id
        return self.GroupLevelApplierTask(self._ws, target_group_id, value=value, property_name=self.property_name)


class EntitlementsSupport(GroupLevelSupport):
    property_name = "entitlements"


class RolesSupport(GroupLevelSupport):
    property_name = "roles"


class PermissionsSupport(BaseSupport):
    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        mentioned_groups = [
            acl.group_name for acl in self._deserialize(item.raw_object_permissions).access_control_list
        ]
        return any(g in mentioned_groups for g in [info.workspace for info in migration_state.groups])

    @classmethod
    def _serialize(cls, typed: ObjectPermissions) -> str:
        return json.dumps(typed.as_dict())

    @classmethod
    def _deserialize(cls, raw: str) -> ObjectPermissions:
        return ObjectPermissions.from_dict(json.loads(raw))

    class PermissionsCrawlerTask(CrawlerTask):
        def __init__(self, ws: WorkspaceClient, object_id: str, request_type: RequestObjectType):
            self._ws = ws
            self._object_id = object_id
            self._request_type = request_type

        def _safe_get_permissions(
            self, request_object_type: RequestObjectType, object_id: str
        ) -> ObjectPermissions | None:
            try:
                permissions = self._ws.permissions.get(request_object_type, object_id)
                return permissions
            except DatabricksError as e:
                if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                    logger.warning(
                        f"Could not get permissions for {request_object_type} {object_id} due to {e.error_code}"
                    )
                    return None
                else:
                    raise e

        @sleep_and_retry
        @limits(calls=100, period=1)
        def __call__(self, _, __) -> PermissionsInventoryItem | None:
            permissions = self._safe_get_permissions(self._request_type, self._object_id)
            if permissions:
                return PermissionsInventoryItem(
                    object_id=self._object_id,
                    crawler=str(self._request_type),
                    raw_object_permissions=PermissionsSupport._serialize(permissions),
                )

    class PermissionsApplierTask(ApplierTask):
        def __init__(
            self, ws: WorkspaceClient, acl: list[AccessControlRequest], object_id: str, request_type: RequestObjectType
        ):
            self._ws = ws
            self._acl = acl
            self._object_id = object_id
            self._request_type = request_type

        @sleep_and_retry
        @limits(calls=30, period=1)
        def __call__(self, _, __):
            self._ws.permissions.update(self._request_type, self._object_id, self._acl)

    def __init__(
        self, listing_function: Callable, id_attribute: str, ws: WorkspaceClient, request_type: RequestObjectType
    ):
        super().__init__(ws)
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._request_type = request_type

    def get_crawler_tasks(self) -> list[CrawlerTask]:
        objects = self._listing_function()
        return [
            self.PermissionsCrawlerTask(self._ws, getattr(o, self._id_attribute), self._request_type) for o in objects
        ]

    def _prepare_new_acl(
        self, permissions: ObjectPermissions, migration_state: GroupMigrationState, destination: Destination
    ) -> list[AccessControlRequest]:
        _acl = permissions.access_control_list
        acl_requests = []

        for _item in _acl:
            # TODO: we have a double iteration over migration_state.groups
            #  (also by migration_state.get_by_workspace_group_name).
            #  Has to be be fixed by iterating just on .groups
            if _item.group_name in [g.workspace.display_name for g in migration_state.groups]:
                migration_info = migration_state.get_by_workspace_group_name(_item.group_name)
                assert migration_info is not None, f"Group {_item.group_name} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, destination)
                _item.group_name = destination_group.display_name
                _reqs = [
                    AccessControlRequest(
                        group_name=_item.group_name,
                        service_principal_name=_item.service_principal_name,
                        user_name=_item.user_name,
                        permission_level=p.permission_level,
                    )
                    for p in _item.all_permissions
                    if not p.inherited
                ]
                acl_requests.extend(_reqs)

        return acl_requests

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> ApplierTask:
        permissions = PermissionsSupport._deserialize(item.raw_object_permissions)
        new_acl = self._prepare_new_acl(permissions, migration_state, destination)
        return self.PermissionsApplierTask(self._ws, new_acl, item.object_id, self._request_type)


class SqlPermissionsSupport(BaseSupport):
    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        mentioned_groups = [
            acl.group_name for acl in self._deserialize(item.raw_object_permissions).access_control_list
        ]
        return any(g in mentioned_groups for g in [info.workspace for info in migration_state.groups])

    def __init__(
        self, ws: WorkspaceClient, listing_function: Callable, id_attribute: str, object_type: sql.ObjectTypePlural
    ):
        super().__init__(ws)
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._object_type = object_type

    class SqlPermissionsCrawlerTask(CrawlerTask):
        def __init__(self, ws: WorkspaceClient, object_type: sql.ObjectTypePlural, object_id: str):
            self._ws = ws
            self._object_type = object_type
            self._object_id = object_id

        def _safe_get_dbsql_permissions(
            self, object_type: sql.ObjectTypePlural, object_id: str
        ) -> sql.GetResponse | None:
            try:
                permissions = self._ws.dbsql_permissions.get(object_type, object_id)
                return permissions
            except DatabricksError as e:
                if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                    logger.warning(f"Could not get permissions for {object_type} {object_id} due to {e.error_code}")
                    return None
                else:
                    raise e

        @sleep_and_retry
        @limits(calls=100, period=1)
        def __call__(self, _, __) -> PermissionsInventoryItem | None:
            permissions = self._safe_get_dbsql_permissions(self._object_type, self._object_id)
            if permissions:
                return PermissionsInventoryItem(
                    object_id=self._object_id,
                    crawler=str(self._object_type),
                    raw_object_permissions=SqlPermissionsSupport._serialize(permissions),
                )

    class SqlPermissionsApplierTask(ApplierTask):
        def __init__(
            self, ws: WorkspaceClient, acl: list[sql.AccessControl], object_type: sql.ObjectTypePlural, object_id: str
        ):
            self._ws = ws
            self._acl = acl
            self._object_type = object_type
            self._object_id = object_id

        @sleep_and_retry
        @limits(calls=30, period=1)
        def __call__(self, _, __):
            self._ws.dbsql_permissions.set(self._object_type, self._object_id, self._acl)

    def get_crawler_tasks(self) -> list[CrawlerTask]:
        objects = self._listing_function()
        return [
            self.SqlPermissionsCrawlerTask(self._ws, self._object_type, getattr(o, self._id_attribute)) for o in objects
        ]

    def _prepare_new_acl(
        self, acl: list[sql.AccessControl], migration_state: GroupMigrationState, destination: Destination
    ) -> list[sql.AccessControl]:
        acl_requests: list[sql.AccessControl] = []

        for acl_request in acl:
            if acl_request.group_name in [g.workspace.display_name for g in migration_state.groups]:
                migration_info = migration_state.get_by_workspace_group_name(acl_request.group_name)
                assert (
                    migration_info is not None
                ), f"Group {acl_request.group_name} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, destination)
                acl_request.group_name = destination_group.display_name
                acl_requests.append(acl_request)
            else:
                # no changes shall be applied
                acl_requests.append(acl_request)

        return acl_requests

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> ApplierTask:
        permissions = self._deserialize(item.raw_object_permissions)
        new_acl = self._prepare_new_acl(permissions.access_control_list, migration_state, destination)
        return self.SqlPermissionsApplierTask(self._ws, new_acl, self._object_type, item.object_id)

    @classmethod
    def _serialize(cls, typed: sql.GetResponse) -> str:
        return json.dumps(typed.as_dict())

    @classmethod
    def _deserialize(cls, raw: str) -> sql.GetResponse:
        return sql.GetResponse.from_dict(json.loads(raw))
