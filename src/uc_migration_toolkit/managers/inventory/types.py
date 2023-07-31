import json

import pandas as pd
from databricks.sdk.service.iam import ObjectPermissions
from databricks.sdk.service.workspace import AclItem as SdkAclItem
from databricks.sdk.service.workspace import AclPermission as SdkAclPermission
from pydantic import BaseModel
from pydantic.tools import parse_obj_as

from uc_migration_toolkit.generic import StrEnum


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


class SqlRequestObjectType(StrEnum):
    ALERTS = "alerts"
    DASHBOARDS = "dashboards"
    DATA_SOURCES = "data-sources"
    QUERIES = "queries"

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

    def __repr__(self):
        return self.value


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


class PermissionsInventoryItem(BaseModel):
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType | SqlRequestObjectType | None
    raw_object_permissions: str

    @property
    def object_permissions(self) -> dict:
        return json.loads(self.raw_object_permissions)

    @property
    def typed_object_permissions(self) -> ObjectPermissions | AclItemsContainer | RolesAndEntitlements:
        if self.logical_object_type == LogicalObjectType.SECRET_SCOPE:
            return parse_obj_as(AclItemsContainer, self.object_permissions)
        elif self.logical_object_type in [LogicalObjectType.ROLES, LogicalObjectType.ENTITLEMENTS]:
            return parse_obj_as(RolesAndEntitlements, self.object_permissions)
        else:
            return ObjectPermissions.from_dict(self.object_permissions)

    @staticmethod
    def from_pandas(source: pd.DataFrame) -> list["PermissionsInventoryItem"]:
        items = source.to_dict(orient="records")
        return [PermissionsInventoryItem(**item) for item in items]
