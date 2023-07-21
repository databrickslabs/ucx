import enum

from pydantic import BaseModel


class StrEnum(str, enum.Enum):  # re-exported for compatability with older python versions
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, str | enum.auto):
            msg = f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            raise TypeError(msg)
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)

    def _generate_next_value_(name, *_):  # noqa: N805
        return name


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
    CLUSTER = "CLUSTER"

    def __repr__(self):
        return self.value


class PermissionsInventoryItem(BaseModel):
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType | SqlRequestObjectType
    object_permissions: dict
