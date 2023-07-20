from enum import StrEnum

from pydantic import BaseModel


class RequestObjectType(StrEnum):
    AUTHORIZATION = "authorization"  # tokens and passwords are here too!
    CLUSTERS = "clusters"
    CLUSTER_POLICIES = "CLUSTER-policies"
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


class SqlRequestObjectType(StrEnum):
    ALERTS = "alerts"
    DASHBOARDS = "dashboards"
    DATA_SOURCES = "data-sources"
    QUERIES = "queries"


class LogicalObjectType(StrEnum):
    CLUSTER = "CLUSTER"


class PermissionsInventoryItem(BaseModel):
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType | SqlRequestObjectType
    object_permissions: dict
