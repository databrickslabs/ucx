from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from databricks.sdk.service.iam import ObjectPermissions
from databricks.sdk.service.sql import GetResponse as SqlPermissions

from databricks.labs.ucx.inventory.types import (
    AclItemsContainer,
    PermissionsInventoryItem,
    RolesAndEntitlements,
)

ObjectType = TypeVar("ObjectType")


class BaseRelevanceIdentifier(ABC, Generic[ObjectType]):
    @abstractmethod
    def is_relevant_to_groups(self, acl_payload: ObjectType, groups: list[str]) -> bool:
        """
        This method should return True if the item is relevant to the given groups, False otherwise.
        """


class SecretScopeRelevanceIdentifier(BaseRelevanceIdentifier[AclItemsContainer]):
    def is_relevant_to_groups(self, acl_payload: AclItemsContainer, groups: list[str]) -> bool:
        return any(acl.principal in groups for acl in acl_payload.acls)


class RolesAndEntitlementsRelevanceIdentifier(BaseRelevanceIdentifier[RolesAndEntitlements]):
    def is_relevant_to_groups(self, acl_payload: RolesAndEntitlements, groups: list[str]) -> bool:
        return any(g in acl_payload.group_name for g in groups)


class ObjectPermissionsRelevanceIdentifier(BaseRelevanceIdentifier[ObjectPermissions]):
    def is_relevant_to_groups(self, acl_payload: ObjectPermissions, groups: list[str]) -> bool:
        mentioned_groups = [acl.group_name for acl in acl_payload.access_control_list]
        return any(g in mentioned_groups for g in groups)


class SqlPermissionsRelevanceIdentifier(BaseRelevanceIdentifier[SqlPermissions]):
    def is_relevant_to_groups(self, acl_payload: SqlPermissions, groups: list[str]) -> bool:
        mentioned_groups = [acl.group_name for acl in acl_payload.access_control_list]
        return any(g in mentioned_groups for g in groups)


def is_item_relevant_to_groups(item: PermissionsInventoryItem, groups: list[str]) -> bool:
    """
    This method verifies if the given item is relevant to the given groups.
    We use it to filter out only the relevant items from the inventory table.
    :param item:
    :param groups:
    :return:
    """
    typed_acl_payload = item.typed_object_permissions
    if isinstance(typed_acl_payload, ObjectPermissions):
        return ObjectPermissionsRelevanceIdentifier().is_relevant_to_groups(typed_acl_payload, groups)
    elif isinstance(typed_acl_payload, SqlPermissions):
        return SqlPermissionsRelevanceIdentifier().is_relevant_to_groups(typed_acl_payload, groups)
    elif isinstance(typed_acl_payload, AclItemsContainer):
        return SecretScopeRelevanceIdentifier().is_relevant_to_groups(typed_acl_payload, groups)
    elif isinstance(typed_acl_payload, RolesAndEntitlements):
        return RolesAndEntitlementsRelevanceIdentifier().is_relevant_to_groups(typed_acl_payload, groups)
    else:
        msg = f"Unknown type of the object payload: {type(typed_acl_payload)}"
        raise NotImplementedError(msg)
