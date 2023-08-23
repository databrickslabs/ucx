import json
from collections.abc import Iterator
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ObjectPermissions
from pydantic.v1 import parse_obj_as

from databricks.labs.ucx.config import InventoryConfig
from databricks.labs.ucx.generic import StrEnum
from databricks.labs.ucx.inventory.types import AclItemsContainer, RolesAndEntitlements
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.tacl._internal import CrawlerBase


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


@dataclass
class WorkspacePermissions:
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: StrEnum
    raw_object_permissions: str

    @property
    def object_permissions(self) -> dict:
        return json.loads(self.raw_object_permissions)

    @property
    def typed_object_permissions(
        self,
    ) -> ObjectPermissions | AclItemsContainer | RolesAndEntitlements:  # TODO: make them separate top-level fields
        if self.logical_object_type == LogicalObjectType.SECRET_SCOPE:
            return parse_obj_as(AclItemsContainer, self.object_permissions)
        elif self.logical_object_type in [LogicalObjectType.ROLES, LogicalObjectType.ENTITLEMENTS]:
            return parse_obj_as(RolesAndEntitlements, self.object_permissions)
        else:
            return ObjectPermissions.from_dict(self.object_permissions)


class WorkspaceInventory(CrawlerBase):
    def __init__(self, config: InventoryConfig, ws: WorkspaceClient):
        super().__init__(ws, config.warehouse_id, config.catalog, config.database, "workspace_objects")
        self.config = config

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self._full_name}")
        self._exec(f"DROP TABLE IF EXISTS {self._full_name}")
        logger.info("Inventory table cleanup complete")

    def save(self, items: list[WorkspacePermissions]):
        logger.info(f"Saving {len(items)} items to {self._full_name}")
        self._append_records(WorkspacePermissions, items)

    def load_all(self) -> Iterator[WorkspacePermissions]:
        logger.info(f"Loading inventory table {self._full_name}")
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield WorkspacePermissions(*row)

    @staticmethod
    def _is_item_relevant_to_groups(item: WorkspacePermissions, groups: list[str]) -> bool:
        if item.logical_object_type == LogicalObjectType.SECRET_SCOPE:
            _acl_container: AclItemsContainer = item.typed_object_permissions
            return any(acl_item.principal in groups for acl_item in _acl_container.acls)

        elif isinstance(item.request_object_type, RequestObjectType):
            _ops: ObjectPermissions = item.typed_object_permissions
            mentioned_groups = [acl.group_name for acl in _ops.access_control_list]
            return any(g in mentioned_groups for g in groups)

        elif item.logical_object_type in [LogicalObjectType.ENTITLEMENTS, LogicalObjectType.ROLES]:
            return any(g in item.object_id for g in groups)

        else:
            msg = f"Logical object type {item.logical_object_type} is not supported"
            raise NotImplementedError(msg)

    def load_for_groups(self, groups: list[str]) -> list[WorkspacePermissions]:
        logger.info(f"Loading inventory table {self._full_name} and filtering it to relevant groups")
        all_items = list(self.load_all())
        filtered_items = [item for item in all_items if self._is_item_relevant_to_groups(item, groups)]
        logger.info(f"Found {len(filtered_items)} items relevant to the groups among {len(all_items)} items")
        return filtered_items
