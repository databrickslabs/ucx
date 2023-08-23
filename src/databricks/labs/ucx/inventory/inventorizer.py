import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from functools import partial
from typing import Generic, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.iam import AccessControlResponse, Group, ObjectPermissions
from databricks.sdk.service.ml import ModelDatabricks
from databricks.sdk.service.workspace import (
    AclItem,
    ObjectInfo,
    ObjectType,
    SecretScope,
)

from databricks.labs.ucx.inventory.listing import WorkspaceListing
from databricks.labs.ucx.inventory.types import (
    AclItemsContainer,
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)
from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.utils import ProgressReporter, ThreadedExecution

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

    def __init__(
        self,
        ws: ImprovedWorkspaceClient,
        logical_object_type: LogicalObjectType,
        request_object_type: RequestObjectType,
        listing_function: Callable[..., Iterator[InventoryObject]],
        id_attribute: str,
        permissions_function: Callable[..., ObjectPermissions] | None = None,
    ):
        self._ws = ws
        self._logical_object_type = logical_object_type
        self._request_object_type = request_object_type
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._permissions_function = permissions_function if permissions_function else self._safe_get_permissions
        self._objects: list[InventoryObject] = []

    def _safe_get_permissions(self, request_object_type: RequestObjectType, object_id: str) -> ObjectPermissions | None:
        try:
            permissions = self._ws.get_permissions(request_object_type, object_id)
            return permissions
        except DatabricksError as e:
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {request_object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise e

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

    def __init__(self, ws: ImprovedWorkspaceClient):
        self._ws = ws
        self._tokens_acl = []
        self._passwords_acl = []

    def _preload_tokens(self):
        try:
            # TODO: rewrite with self._ws.token_management.get_token_permissions().access_control_list
            return self._ws.api_client.do("GET", "/api/2.0/preview/permissions/authorization/tokens").get(
                "access_control_list", []
            )
        except DatabricksError as e:
            logger.warning("Cannot load token permissions due to error:")
            logger.warning(e)
            return []

    def _preload_passwords(self):
        try:
            # TODO: rewrite with return self._ws.users.get_password_permissions().access_control_list
            return self._ws.api_client.do("GET", "/api/2.0/preview/permissions/authorization/passwords").get(
                "access_control_list", []
            )
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

    def __init__(self, ws: ImprovedWorkspaceClient):
        self._ws = ws
        self._scopes = ws.secrets.list_scopes()

    def _get_acls_for_scope(self, scope: SecretScope) -> Iterator[AclItem]:
        return self._ws.secrets.list_acls(scope.name)

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

    def __init__(self, ws: ImprovedWorkspaceClient, num_threads=20, start_path: str | None = "/"):
        self._ws = ws
        self.listing = WorkspaceListing(
            ws,
            num_threads=num_threads,
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
                permissions = self._ws.get_permissions(
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

    def __init__(self, ws: ImprovedWorkspaceClient, migration_state: GroupMigrationState):
        self._ws = ws
        self._migration_state = migration_state
        self._group_info: list[Group] = []

    def preload(self):
        logger.info("Please note that group roles and entitlements will be ONLY inventorized for migration groups")
        self._group_info: list[Group] = [
            # TODO: why do we load group twice from platform? this really looks unnecessary
            self._ws.groups.get(id=g.workspace.id)
            for g in self._migration_state.groups
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


def models_listing(ws: WorkspaceClient):
    def inner() -> Iterator[ModelDatabricks]:
        for model in ws.model_registry.list_models():
            model_with_id = ws.model_registry.get_model(model.name).registered_model_databricks
            yield model_with_id

    return inner


def experiments_listing(ws: WorkspaceClient):
    def inner() -> Iterator[ModelDatabricks]:
        for experiment in ws.experiments.list_experiments():
            nb_tag = [t for t in experiment.tags if t.key == "mlflow.experimentType" and t.value == "NOTEBOOK"]
            if not nb_tag:
                yield experiment

    return inner


class Inventorizers:
    @staticmethod
    def provide(ws: ImprovedWorkspaceClient, migration_state: GroupMigrationState, num_threads: int):
        return [
            RolesAndEntitlementsInventorizer(ws, migration_state),
            TokensAndPasswordsInventorizer(ws),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                listing_function=ws.clusters.list,
                id_attribute="cluster_id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.INSTANCE_POOL,
                request_object_type=RequestObjectType.INSTANCE_POOLS,
                listing_function=ws.instance_pools.list,
                id_attribute="instance_pool_id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.CLUSTER_POLICY,
                request_object_type=RequestObjectType.CLUSTER_POLICIES,
                listing_function=ws.cluster_policies.list,
                id_attribute="policy_id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.PIPELINE,
                request_object_type=RequestObjectType.PIPELINES,
                listing_function=ws.pipelines.list_pipelines,
                id_attribute="pipeline_id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.JOB,
                request_object_type=RequestObjectType.JOBS,
                listing_function=ws.jobs.list,
                id_attribute="job_id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.EXPERIMENT,
                request_object_type=RequestObjectType.EXPERIMENTS,
                listing_function=experiments_listing(ws),
                id_attribute="experiment_id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.MODEL,
                request_object_type=RequestObjectType.REGISTERED_MODELS,
                listing_function=models_listing(ws),
                id_attribute="id",
            ),
            StandardInventorizer(
                ws,
                logical_object_type=LogicalObjectType.WAREHOUSE,
                request_object_type=RequestObjectType.SQL_WAREHOUSES,
                listing_function=ws.warehouses.list,
                id_attribute="id",
            ),
            SecretScopeInventorizer(ws),
            WorkspaceInventorizer(ws, num_threads=num_threads),
        ]
