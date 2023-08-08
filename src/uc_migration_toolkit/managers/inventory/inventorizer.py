import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from functools import partial
from typing import Generic, TypeVar

from databricks.sdk.core import DatabricksError
from databricks.sdk.service.iam import AccessControlResponse, Group, ObjectPermissions
from databricks.sdk.service.sql import (
    Alert,
    Dashboard,
    GetResponse,
    ObjectTypePlural,
    Query,
)
from databricks.sdk.service.workspace import (
    AclItem,
    ObjectInfo,
    ObjectType,
    SecretScope,
)

from uc_migration_toolkit.managers.inventory.listing import (
    CustomListing,
    WorkspaceListing,
)
from uc_migration_toolkit.managers.inventory.types import (
    AclItemsContainer,
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.groups_info import MigrationGroupsProvider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import ProgressReporter, ThreadedExecution

InventoryObject = TypeVar("InventoryObject")


class BaseInventorizer(ABC, Generic[InventoryObject]):
    @property
    @abstractmethod
    def logical_object_types(self) -> list[LogicalObjectType]:
        """Logical object types that this inventorizer can handle"""

    @abstractmethod
    def preload(self):
        """Any preloading activities should happen here"""

    @abstractmethod
    def inventorize(self) -> list[PermissionsInventoryItem]:
        """Any inventorization activities should happen here"""


class StandardInventorizer(BaseInventorizer[InventoryObject]):
    """
    Standard means that it can collect using the default listing/permissions function without any additional logic.
    """

    @property
    def logical_object_types(self) -> list[LogicalObjectType]:
        return [self._logical_object_type]

    @staticmethod
    def safe_get_permissions(request_object_type: RequestObjectType, object_id: str) -> ObjectPermissions | None:
        try:
            permissions = provider.ws.get_permissions(request_object_type, object_id)
            return permissions
        except DatabricksError as e:
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {request_object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise e

    def __init__(
        self,
        logical_object_type: LogicalObjectType,
        request_object_type: RequestObjectType,
        listing_function: Callable[..., Iterator[InventoryObject]],
        id_attribute: str,
        permissions_function: Callable[..., ObjectPermissions] | None = None,
    ):
        self._logical_object_type = logical_object_type
        self._request_object_type = request_object_type
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._permissions_function = permissions_function if permissions_function else self.safe_get_permissions
        self._objects: list[InventoryObject] = []

    @property
    def logical_object_type(self) -> LogicalObjectType:
        return self._logical_object_type

    def preload(self):
        logger.info(f"Listing objects with type {self._request_object_type}...")
        self._objects = list(self._listing_function())
        logger.info(f"Object metadata prepared for {len(self._objects)} objects.")

    def _process_single_object(self, _object: InventoryObject) -> PermissionsInventoryItem | None:
        object_id = str(getattr(_object, self._id_attribute))
        permissions = self._permissions_function(self._request_object_type, object_id)
        if permissions:
            inventory_item = PermissionsInventoryItem(
                object_id=object_id,
                logical_object_type=self._logical_object_type,
                request_object_type=self._request_object_type,
                raw_object_permissions=json.dumps(permissions.as_dict()),
            )
            return inventory_item

    def inventorize(self):
        logger.info(f"Fetching permissions for {len(self._objects)} objects...")

        executables = [partial(self._process_single_object, _object) for _object in self._objects]
        threaded_execution = ThreadedExecution[PermissionsInventoryItem](executables)
        collected = [item for item in threaded_execution.run() if item is not None]
        logger.info(f"Permissions fetched for {len(collected)} objects of type {self._request_object_type}")
        return collected


class TokensAndPasswordsInventorizer(BaseInventorizer[InventoryObject]):
    @property
    def logical_object_types(self) -> list[LogicalObjectType]:
        return [LogicalObjectType.TOKEN, LogicalObjectType.PASSWORD]

    def __init__(self):
        self._tokens_acl = []
        self._passwords_acl = []

    @staticmethod
    def _preload_tokens():
        try:
            return provider.ws.get_tokens().get("access_control_list", [])
        except DatabricksError as e:
            logger.warning("Cannot load token permissions due to error:")
            logger.warning(e)
            return []

    @staticmethod
    def _preload_passwords():
        try:
            return provider.ws.get_passwords().get("access_control_list", [])
        except DatabricksError as e:
            logger.error("Cannot load password permissions due to error:")
            logger.error(e)
            return []

    def preload(self):
        self._tokens_acl = [AccessControlResponse.from_dict(acl) for acl in self._preload_tokens()]
        self._passwords_acl = [AccessControlResponse.from_dict(acl) for acl in self._preload_passwords()]

    def inventorize(self) -> list[PermissionsInventoryItem]:
        results = []

        if self._passwords_acl:
            results.append(
                PermissionsInventoryItem(
                    object_id="passwords",
                    logical_object_type=LogicalObjectType.PASSWORD,
                    request_object_type=RequestObjectType.AUTHORIZATION,
                    raw_object_permissions=json.dumps(
                        ObjectPermissions(
                            object_id="passwords", object_type="authorization", access_control_list=self._passwords_acl
                        ).as_dict()
                    ),
                )
            )

        if self._tokens_acl:
            results.append(
                PermissionsInventoryItem(
                    object_id="tokens",
                    logical_object_type=LogicalObjectType.TOKEN,
                    request_object_type=RequestObjectType.AUTHORIZATION,
                    raw_object_permissions=json.dumps(
                        ObjectPermissions(
                            object_id="tokens", object_type="authorization", access_control_list=self._tokens_acl
                        ).as_dict()
                    ),
                )
            )
        return results


class SecretScopeInventorizer(BaseInventorizer[InventoryObject]):
    @property
    def logical_object_types(self) -> list[LogicalObjectType]:
        return [LogicalObjectType.SECRET_SCOPE]

    def __init__(self):
        self._scopes = provider.ws.secrets.list_scopes()

    @staticmethod
    def _get_acls_for_scope(scope: SecretScope) -> Iterator[AclItem]:
        return provider.ws.secrets.list_acls(scope.name)

    def _prepare_permissions_inventory_item(self, scope: SecretScope) -> PermissionsInventoryItem:
        acls = self._get_acls_for_scope(scope)
        acls_container = AclItemsContainer.from_sdk(list(acls))

        return PermissionsInventoryItem(
            object_id=scope.name,
            logical_object_type=LogicalObjectType.SECRET_SCOPE,
            request_object_type=None,
            raw_object_permissions=json.dumps(acls_container.model_dump(mode="json")),
        )

    def inventorize(self) -> list[PermissionsInventoryItem]:
        executables = [partial(self._prepare_permissions_inventory_item, scope) for scope in self._scopes]
        results = ThreadedExecution[PermissionsInventoryItem](executables).run()
        logger.info(f"Permissions fetched for {len(results)} objects of type {LogicalObjectType.SECRET_SCOPE}")
        return results

    def preload(self):
        pass


class WorkspaceInventorizer(BaseInventorizer[InventoryObject]):
    @property
    def logical_object_types(self) -> list[LogicalObjectType]:
        return [LogicalObjectType.NOTEBOOK, LogicalObjectType.DIRECTORY, LogicalObjectType.REPO, LogicalObjectType.FILE]

    def __init__(self, start_path: str | None = "/"):
        self.listing = WorkspaceListing(
            provider.ws,
            num_threads=config_provider.config.num_threads,
            with_directories=False,
        )
        self._start_path = start_path

    def preload(self):
        pass

    @staticmethod
    def __convert_object_type_to_request_type(_object: ObjectInfo) -> RequestObjectType | None:
        match _object.object_type:
            case ObjectType.NOTEBOOK:
                return RequestObjectType.NOTEBOOKS
            case ObjectType.DIRECTORY:
                return RequestObjectType.DIRECTORIES
            case ObjectType.LIBRARY:
                return None
            case ObjectType.REPO:
                return RequestObjectType.REPOS
            case ObjectType.FILE:
                return RequestObjectType.FILES
            # silent handler for experiments - they'll be inventorized by the experiments manager
            case None:
                return None

    @staticmethod
    def __convert_request_object_type_to_logical_type(request_object_type: RequestObjectType) -> LogicalObjectType:
        match request_object_type:
            case RequestObjectType.NOTEBOOKS:
                return LogicalObjectType.NOTEBOOK
            case RequestObjectType.DIRECTORIES:
                return LogicalObjectType.DIRECTORY
            case RequestObjectType.REPOS:
                return LogicalObjectType.REPO
            case RequestObjectType.FILES:
                return LogicalObjectType.FILE

    def _convert_result_to_permission_item(self, _object: ObjectInfo) -> PermissionsInventoryItem | None:
        request_object_type = self.__convert_object_type_to_request_type(_object)
        if not request_object_type:
            return
        else:
            try:
                permissions = provider.ws.get_permissions(
                    request_object_type=request_object_type, request_object_id=_object.object_id
                )
            except DatabricksError as e:
                if e.error_code in ["PERMISSION_DENIED", "RESOURCE_NOT_FOUND"]:
                    logger.warning(f"Cannot load permissions for {_object.path} due to error {e.error_code}")
                    return
                else:
                    raise e

            if permissions:
                inventory_item = PermissionsInventoryItem(
                    object_id=str(_object.object_id),
                    logical_object_type=self.__convert_request_object_type_to_logical_type(request_object_type),
                    request_object_type=request_object_type,
                    raw_object_permissions=json.dumps(permissions.as_dict()),
                )
                return inventory_item

    def inventorize(self) -> list[PermissionsInventoryItem]:
        self.listing.walk(self._start_path)
        executables = [partial(self._convert_result_to_permission_item, _object) for _object in self.listing.results]
        results = ThreadedExecution[PermissionsInventoryItem | None](
            executables,
            progress_reporter=ProgressReporter(
                len(executables), "Fetching permissions for workspace objects - processed: "
            ),
        ).run()
        results = [result for result in results if result]  # empty filter
        logger.info(f"Permissions fetched for {len(results)} workspace objects")
        return results


class RolesAndEntitlementsInventorizer(BaseInventorizer[InventoryObject]):
    @property
    def logical_object_types(self) -> list[LogicalObjectType]:
        return [LogicalObjectType.ROLES, LogicalObjectType.ENTITLEMENTS]

    def __init__(self, migration_provider: MigrationGroupsProvider):
        self._migration_provider = migration_provider
        self._group_info: list[Group] = []

    def preload(self):
        logger.info("Please note that group roles and entitlements will be ONLY inventorized for migration groups")
        self._group_info: list[Group] = [
            provider.ws.groups.get(id=g.workspace.id) for g in self._migration_provider.groups
        ]
        logger.info("Group roles and entitlements preload completed")

    def inventorize(self) -> list[PermissionsInventoryItem]:
        _items = []

        for group in self._group_info:
            # add safe-getters
            group_roles = group.roles if group.roles else []
            group_entitlements = group.entitlements if group.entitlements else []

            roles = [r.as_dict() for r in group_roles]
            entitlements = [e.as_dict() for e in group_entitlements]

            inventory_item = PermissionsInventoryItem(
                object_id=group.display_name,
                logical_object_type=LogicalObjectType.ROLES,
                request_object_type=None,
                raw_object_permissions=json.dumps({"roles": roles, "entitlements": entitlements}),
            )
            _items.append(inventory_item)

        return _items


class DBSQLInventorizer(BaseInventorizer[InventoryObject]):
    def __init__(self):
        self._alerts: list[Alert] = []
        self._dashboards: list[Dashboard] = []
        self._queries: list[Query] = []

    @property
    def logical_object_types(self) -> list[LogicalObjectType]:
        return [LogicalObjectType.ALERT, LogicalObjectType.DASHBOARD, LogicalObjectType.QUERY]

    @staticmethod
    def safe_get_dbsql_permissions(object_type: ObjectTypePlural, request_object_id: str) -> GetResponse | None:
        try:
            permissions = provider.ws.get_dbsql_permissions(
                object_type=object_type, request_object_id=request_object_id
            )
            return permissions
        except DatabricksError as e:
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {object_type} {request_object_id} due to {e.error_code}")
                return None
            else:
                raise e

    def preload(self):
        logger.info("Listing DBSQL Alerts...")
        self._alerts = list(provider.ws.alerts.list())
        logger.info(f"Finished listing of DBSQL Alerts, found {len(self._alerts)} objects")

        logger.info("Listing DBSQL Dashboards")
        self._dashboards = list(provider.ws.dashboards.list())
        logger.info(f"Finished listing of DBSQL Dashboards, found {len(self._dashboards)} objects")

        logger.info("Listing DBSQL Queries")
        self._queries = list(provider.ws.queries.list())
        logger.info(f"Finished listing of DBSQL Queries, found {len(self._queries)} queries")

    @staticmethod
    def get_object_type(_obj: Alert | Dashboard | Query) -> ObjectTypePlural:
        if isinstance(_obj, Alert):
            return ObjectTypePlural.ALERTS
        elif isinstance(_obj, Dashboard):
            return ObjectTypePlural.DASHBOARDS
        elif isinstance(_obj, Query):
            return ObjectTypePlural.QUERIES
        else:
            msg = f"Unknown object type {_obj}"
            raise ValueError(msg)

    def _get_object_permissions(self, _obj: Alert | Dashboard | Query) -> PermissionsInventoryItem | None:
        if isinstance(_obj, Alert):
            logical_object_type = LogicalObjectType.ALERT
        elif isinstance(_obj, Dashboard):
            logical_object_type = LogicalObjectType.DASHBOARD
        elif isinstance(_obj, Query):
            logical_object_type = LogicalObjectType.QUERY
        else:
            msg = f"Unknown object type {_obj}"
            raise ValueError(msg)

        request_object_type = self.get_object_type(_obj)
        permissions = self.safe_get_dbsql_permissions(object_type=request_object_type, request_object_id=_obj.id)
        if permissions:
            return PermissionsInventoryItem(
                object_id=_obj.id,
                logical_object_type=logical_object_type,
                request_object_type=request_object_type,
                raw_object_permissions=json.dumps(permissions.as_dict()),
            )

    def inventorize(self) -> list[PermissionsInventoryItem]:
        logger.info("DBSQL inventorization started")
        logger.info("Collecting permissions for DBSQL objects")
        dbsql_objects = self._alerts + self._dashboards + self._queries
        executables = [partial(self._get_object_permissions, _obj) for _obj in dbsql_objects]
        results = ThreadedExecution[PermissionsInventoryItem](executables).run()
        non_empty_results = [result for result in results if result]
        logger.info(f"DBSQL inventorization completed. {len(non_empty_results)} objects inventorized")
        return non_empty_results


class Inventorizers:
    @staticmethod
    def provide(migration_provider: MigrationGroupsProvider):
        return [
            RolesAndEntitlementsInventorizer(migration_provider),
            TokensAndPasswordsInventorizer(),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                listing_function=provider.ws.clusters.list,
                id_attribute="cluster_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.INSTANCE_POOL,
                request_object_type=RequestObjectType.INSTANCE_POOLS,
                listing_function=provider.ws.instance_pools.list,
                id_attribute="instance_pool_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.CLUSTER_POLICY,
                request_object_type=RequestObjectType.CLUSTER_POLICIES,
                listing_function=provider.ws.cluster_policies.list,
                id_attribute="policy_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.PIPELINE,
                request_object_type=RequestObjectType.PIPELINES,
                listing_function=provider.ws.pipelines.list_pipelines,
                id_attribute="pipeline_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.JOB,
                request_object_type=RequestObjectType.JOBS,
                listing_function=provider.ws.jobs.list,
                id_attribute="job_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.EXPERIMENT,
                request_object_type=RequestObjectType.EXPERIMENTS,
                listing_function=CustomListing.list_experiments,
                id_attribute="experiment_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.MODEL,
                request_object_type=RequestObjectType.REGISTERED_MODELS,
                listing_function=CustomListing.list_models,
                id_attribute="id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.WAREHOUSE,
                request_object_type=RequestObjectType.SQL_WAREHOUSES,
                listing_function=provider.ws.warehouses.list,
                id_attribute="id",
            ),
            SecretScopeInventorizer(),
            WorkspaceInventorizer(),
        ]
