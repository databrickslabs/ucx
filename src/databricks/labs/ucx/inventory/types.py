import json
from dataclasses import asdict, dataclass

import pandas as pd
from databricks.sdk.service.iam import ObjectPermissions
from databricks.sdk.service.sql import GetResponse as SqlPermissions
from databricks.sdk.service.sql import ObjectTypePlural as SqlRequestObjectType
from databricks.sdk.service.workspace import AclItem as SdkAclItem
from databricks.sdk.service.workspace import AclPermission as SdkAclPermission

from databricks.labs.ucx.generic import StrEnum


class RequestObjectType(StrEnum):
    AUTHORIZATION = "authorization"  # tokens and passwords are here too!
    CLUSTERS = "clusters"
    CLUSTER_POLICIES = "cluster-policies"
    DIRECTORIES = "directories"
    EXPERIMENTS = "experiments"
    FILES = "files"
    INSTANCE_POOLS = "instance-pools"
    JOBS = "jobs"
    NOTEBOOKS = "notebooks"
    PIPELINES = "pipelines"
    REGISTERED_MODELS = "registered-models"
    REPOS = "repos"
    SERVING_ENDPOINTS = "serving-endpoints"
    SQL_WAREHOUSES = "sql/warehouses"  # / is not a typo, it's the real object type

    def __repr__(self):
        return self.value


class LogicalObjectType(StrEnum):
    ENTITLEMENTS = "ENTITLEMENTS"
    ROLES = "ROLES"
    FILE = "FILE"
    REPO = "REPO"
    DIRECTORY = "DIRECTORY"
    NOTEBOOK = "NOTEBOOK"
    SECRET_SCOPE = "SECRET_SCOPE"
    PASSWORD = "PASSWORD"
    TOKEN = "TOKEN"
    WAREHOUSE = "WAREHOUSE"
    MODEL = "MODEL"
    EXPERIMENT = "EXPERIMENT"
    JOB = "JOB"
    PIPELINE = "PIPELINE"
    CLUSTER = "CLUSTER"
    INSTANCE_POOL = "INSTANCE_POOL"
    CLUSTER_POLICY = "CLUSTER_POLICY"

    # DBSQL Objects
    ALERT = "ALERT"
    DASHBOARD = "DASHBOARD"
    QUERY = "QUERY"

    def __repr__(self):
        return self.value


class AclPermission(StrEnum):
    READ = "READ"
    WRITE = "WRITE"
    MANAGE = "MANAGE"


@dataclass
class AclItem:
    principal: str
    permission: AclPermission

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(principal=raw.get("principal", None), permission=AclPermission(raw.get("permission")))


@dataclass
class AclItemsContainer:
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

    @classmethod
    def from_dict(cls, raw: dict) -> "AclItemsContainer":
        return cls(acls=[AclItem.from_dict(a) for a in raw.get("acls", [])])

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class RolesAndEntitlements:
    roles: list
    entitlements: list


@dataclass
class PermissionsInventoryItem:
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType | SqlRequestObjectType | None
    raw_object_permissions: str

    @property
    def object_permissions(self) -> dict:
        return json.loads(self.raw_object_permissions)

    @property
    def typed_object_permissions(self) -> ObjectPermissions | AclItemsContainer | RolesAndEntitlements | SqlPermissions:
        if self.logical_object_type == LogicalObjectType.SECRET_SCOPE:
            return AclItemsContainer.from_dict(self.object_permissions)
        elif self.logical_object_type in [LogicalObjectType.ROLES, LogicalObjectType.ENTITLEMENTS]:
            return RolesAndEntitlements(**self.object_permissions)
        elif self.logical_object_type in [
            LogicalObjectType.ALERT,
            LogicalObjectType.DASHBOARD,
            LogicalObjectType.QUERY,
        ]:
            return SqlPermissions.from_dict(self.object_permissions)
        else:
            return ObjectPermissions.from_dict(self.object_permissions)

    @staticmethod
    def from_pandas(source: pd.DataFrame) -> list["PermissionsInventoryItem"]:
        items = source.to_dict(orient="records")
        return [PermissionsInventoryItem.from_dict(item) for item in items]

    def as_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "PermissionsInventoryItem":
        return cls(
            object_id=raw["object_id"],
            logical_object_type=LogicalObjectType(raw["logical_object_type"]),
            request_object_type=RequestObjectType(raw["request_object_type"])
            if raw.get("request_object_type", None) is not None
            else None,
            raw_object_permissions=raw.get("raw_object_permissions", None),
        )
