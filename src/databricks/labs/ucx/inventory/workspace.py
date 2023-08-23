import json
from collections.abc import Iterator
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ObjectPermissions
from databricks.sdk.service.workspace import AclItem

from databricks.labs.ucx.config import InventoryConfig
from databricks.labs.ucx.generic import StrEnum
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

    # SQL object types
    ALERTS = "alerts"
    DASHBOARDS = "dashboards"
    DATA_SOURCES = "data-sources"
    QUERIES = "queries"

    def __repr__(self):
        return self.value

@dataclass
class RolesAndEntitlements:
    roles: list[str]
    entitlements: list[str]

@dataclass
class WorkspacePermissions:
    object_id: str
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType
    raw_object_permissions: str

    @property
    def _object_permissions(self) -> dict:
        return json.loads(self.raw_object_permissions)

    @property
    def object_permissions(self) -> ObjectPermissions:
        return ObjectPermissions.from_dict(self._object_permissions)

    @property
    def roles_and_entitlements(self) -> RolesAndEntitlements | None:
        if self.logical_object_type not in [LogicalObjectType.ROLES, LogicalObjectType.ENTITLEMENTS]:
            return None
        x = self._object_permissions
        return RolesAndEntitlements(roles=x.get('roles', []), entitlements=x.get('entitlements', []))

    @property
    def secret_scope_acls(self) -> list[AclItem]:
        if self.logical_object_type != LogicalObjectType.SECRET_SCOPE:
            return []
        return [AclItem.from_dict(i) for i in self._object_permissions]

    def _principals_from_secret_scopes(self) -> Iterator[str]:
        for i in self.secret_scope_acls:
            yield i.principal

    def _principals_from_roles(self) -> Iterator[str]:
        if self.logical_object_type not in [LogicalObjectType.ENTITLEMENTS, LogicalObjectType.ROLES]:
            return
        yield self.object_id

    def _principals_from_generic_permissions(self) -> Iterator[str]:
        for x in self.object_permissions.access_control_list:
            # TODO: this does not allow for user-level access
            yield x.group_name

    def is_relevant_to_groups(self, groups: list[str]) -> bool:
        check = {g: True for g in groups}
        for it in [self._principals_from_secret_scopes(),
                   self._principals_from_roles(),
                   self._principals_from_generic_permissions()]:
            for principal in it:
                if check.get(principal, False):
                    return True
        return False


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
        for (object_id, logical_type, request_type, raw) in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield WorkspacePermissions(
                object_id=object_id,
                logical_object_type=LogicalObjectType(logical_type),
                request_object_type=RequestObjectType(request_type) if request_type else None,
                raw_object_permissions=raw)

    def load_for_groups(self, groups: list[str]) -> list[WorkspacePermissions]:
        logger.info(f"Loading inventory table {self._full_name} and filtering it to relevant groups")
        all_items = list(self.load_all())
        filtered_items = [item for item in all_items if item.is_relevant_to_groups(groups)]
        logger.info(f"Found {len(filtered_items)} items relevant to the groups among {len(all_items)} items")
        return filtered_items
