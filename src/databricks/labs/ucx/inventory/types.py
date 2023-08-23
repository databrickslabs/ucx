from databricks.sdk.service.workspace import AclItem as SdkAclItem
from databricks.sdk.service.workspace import AclPermission as SdkAclPermission
from pydantic import BaseModel

from databricks.labs.ucx.generic import StrEnum


class AclPermission(StrEnum):
    READ = "READ"
    WRITE = "WRITE"
    MANAGE = "MANAGE"


class AclItem(BaseModel):
    principal: str
    permission: AclPermission


class AclItemsContainer(BaseModel):
    acls: list[AclItem]

    @staticmethod
    def from_sdk(source: list[SdkAclItem]) -> "AclItemsContainer":
        _typed_acls = [
            AclItem(principal=acl.principal, permission=AclPermission(acl.permission.value)) for acl in source
        ]
        return AclItemsContainer(acls=_typed_acls)

    def to_sdk(self) -> list[SdkAclItem]:
        return [
            SdkAclItem(principal=acl.principal, permission=SdkAclPermission(acl.permission.value)) for acl in self.acls
        ]


class RolesAndEntitlements(BaseModel):
    roles: list
    entitlements: list
