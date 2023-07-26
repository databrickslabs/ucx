import json

import pandas as pd
from databricks.sdk.service.iam import ObjectPermissions
from pydantic import BaseModel

from uc_migration_toolkit.utils import StrEnum


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
    SQL_WAREHOUSES = "sql-warehouses"
    TOKENS = "tokens"

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
    MODEL = "MODEL"
    EXPERIMENT = "EXPERIMENT"
    JOB = "JOB"
    PIPELINE = "PIPELINE"
    CLUSTER = "CLUSTER"
    INSTANCE_POOL = "INSTANCE_POOL"
    CLUSTER_POLICY = "CLUSTER_POLICY"

    def __repr__(self):
        return self.value


class PermissionsInventoryItem(BaseModel):
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType | SqlRequestObjectType
    object_permissions: dict

    @property
    def typed_object_permissions(self) -> ObjectPermissions:
        return ObjectPermissions.from_dict(self.object_permissions)

    @staticmethod
    def from_pandas(source: pd.DataFrame) -> list["PermissionsInventoryItem"]:
        items = source.to_dict(orient="records")

        for item in items:
            item["object_permissions"] = json.loads(item["plain_permissions"])
            item.pop("plain_permissions")

        return [PermissionsInventoryItem(**item) for item in items]
