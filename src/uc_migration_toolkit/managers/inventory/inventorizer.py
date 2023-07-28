import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from functools import partial
from typing import Generic, TypeVar

from databricks.sdk.core import DatabricksError
from databricks.sdk.service.iam import AccessControlResponse, ObjectPermissions
from databricks.sdk.service.workspace import (
    AclItem,
    ObjectInfo,
    ObjectType,
    SecretScope,
)

from uc_migration_toolkit.managers.inventory.listing import WorkspaceListing
from uc_migration_toolkit.managers.inventory.types import (
    AclItemsContainer,
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import ThreadedExecution

InventoryObject = TypeVar("InventoryObject")


class BaseInventorizer(ABC, Generic[InventoryObject]):
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

    def __init__(
        self,
        logical_object_type: LogicalObjectType,
        request_object_type: RequestObjectType,
        listing_function: Callable[..., Iterator[InventoryObject]],
        id_attribute: str,
        permissions_function: Callable[..., ObjectPermissions] | None = None,
    ):
        self._config = config_provider.config.rate_limit
        self._logical_object_type = logical_object_type
        self._request_object_type = request_object_type
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._permissions_function = permissions_function if permissions_function else provider.ws.permissions.get
        self._objects: list[InventoryObject] = []

    @property
    def logical_object_type(self) -> LogicalObjectType:
        return self._logical_object_type

    def preload(self):
        logger.info(f"Listing objects with type {self._request_object_type}...")
        self._objects = list(self._listing_function())
        logger.info(f"Object metadata prepared for {len(self._objects)} objects.")

    def _process_single_object(self, _object: InventoryObject) -> PermissionsInventoryItem:
        object_id = str(getattr(_object, self._id_attribute))
        permissions = self._permissions_function(self._request_object_type, object_id)
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
        collected = threaded_execution.run()
        logger.info(f"Permissions fetched for {len(collected)} objects of type {self._request_object_type}")
        return collected


class TokensAndPasswordsInventorizer(BaseInventorizer[InventoryObject]):
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
    def __init__(self):
        self.listing = WorkspaceListing(
            provider.ws,
            num_threads=config_provider.config.num_threads,
            with_directories=False,
            rate_limit=config_provider.config.rate_limit,
        )

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
            permissions = provider.ws.permissions.get(
                request_object_type=request_object_type, request_object_id=_object.object_id
            )

            inventory_item = PermissionsInventoryItem(
                object_id=str(_object.object_id),
                logical_object_type=self.__convert_request_object_type_to_logical_type(request_object_type),
                request_object_type=request_object_type,
                raw_object_permissions=json.dumps(permissions.as_dict()),
            )
            return inventory_item

    def inventorize(self) -> list[PermissionsInventoryItem]:
        self.listing.walk("/")
        executables = [partial(self._convert_result_to_permission_item, _object) for _object in self.listing.results]
        results = ThreadedExecution[PermissionsInventoryItem | None](executables).run()
        results = [result for result in results if result]
        logger.info(f"Permissions fetched for {len(results)} workspace objects")
        return results
