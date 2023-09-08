import json
from abc import ABC, abstractmethod
from typing import Any
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import (
    ComplexValue,
    Group,
    ObjectPermissions,
    Patch,
    PatchOp,
    PatchSchema,
)
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem


class ClientMixin:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws


class BaseTask:
    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


class CrawlerTask(BaseTask):
    @abstractmethod
    def __call__(self, *args, **kwargs) -> PermissionsInventoryItem:
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
    def get_apply_task(self, item: PermissionsInventoryItem) -> ApplierTask:
        pass


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


class GroupLevelSupport(ABC, BaseSupport):
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

    def get_apply_task(self, item: PermissionsInventoryItem) -> ApplierTask:
        value = self._deserialize(item.raw_object_permissions)
        return self.GroupLevelApplierTask(self._ws, item.object_id, value=value, property_name=self.property_name)


class EntitlementsSupport(GroupLevelSupport):
    property_name = "entitlements"


class RolesSupport(GroupLevelSupport):
    property_name = "roles"


class PermissionsSupport(BaseSupport):
    @classmethod
    def _serialize(cls, typed: ObjectPermissions) -> str:
        pass

    @classmethod
    def _deserialize(cls, raw: str) -> Any:
        pass

    class PermissionsCrawlerTask(CrawlerTask):
        def __init__(self, ws: WorkspaceClient, object_id: str, request_type: PermissionsRequestType):
            self._ws = ws
            self._object_id = object_id
            self._request_type = request_type

        def __call__(self, _, __) -> PermissionsInventoryItem:
            permissions = self._ws.permissions.get(self._request_type, self._object_id)
            return PermissionsInventoryItem(
                object_id=self._object_id,
                crawler=str(self._request_type),
                raw_object_permissions=PermissionsSupport._serialize(permissions),
            )

    def __init__(
        self, listing_function: Callable, id_attribute: str, ws: WorkspaceClient, request_type: PermissionsRequestType
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

    def get_apply_task(self, item: PermissionsInventoryItem) -> ApplierTask:
        pass
