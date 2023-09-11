import json
from collections.abc import Callable
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam, workspace
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.inventory.listing import WorkspaceListing
from databricks.labs.ucx.inventory.types import (
    Destination,
    PermissionsInventoryItem,
    RequestObjectType,
)
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.supports.base import BaseSupport, logger


class PermissionsOp:
    """
    Common methods for permissions.
    Please note that this mixin is stateless BY DESIGN.
    """

    def _is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        mentioned_groups = [
            acl.group_name
            for acl in iam.ObjectPermissions.from_dict(json.loads(item.raw_object_permissions)).access_control_list
        ]
        return any(g in mentioned_groups for g in [info.workspace for info in migration_state.groups])

    def _safe_get_permissions(
        self, ws: WorkspaceClient, request_object_type: RequestObjectType, object_id: str
    ) -> iam.ObjectPermissions | None:
        try:
            permissions = ws.permissions.get(request_object_type, object_id)
            return permissions
        except DatabricksError as e:
            if e.error_code in ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND", "PERMISSION_DENIED"]:
                logger.warning(f"Could not get permissions for {request_object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise e

    def _prepare_new_acl(
        self, permissions: iam.ObjectPermissions, migration_state: GroupMigrationState, destination: Destination
    ) -> list[iam.AccessControlRequest]:
        _acl = permissions.access_control_list
        acl_requests = []

        for _item in _acl:
            # TODO: we have a double iteration over migration_state.groups
            #  (also by migration_state.get_by_workspace_group_name).
            #  Has to be be fixed by iterating just on .groups
            if _item.group_name in [g.workspace.display_name for g in migration_state.groups]:
                migration_info = migration_state.get_by_workspace_group_name(_item.group_name)
                assert migration_info is not None, f"Group {_item.group_name} is not in the migration groups provider"
                destination_group: iam.Group = getattr(migration_info, destination)
                _item.group_name = destination_group.display_name
                _reqs = [
                    iam.AccessControlRequest(
                        group_name=_item.group_name,
                        service_principal_name=_item.service_principal_name,
                        user_name=_item.user_name,
                        permission_level=p.permission_level,
                    )
                    for p in _item.all_permissions
                    if not p.inherited
                ]
                acl_requests.extend(_reqs)

        return acl_requests

    @sleep_and_retry
    @limits(calls=30, period=1)
    def _applier_task(
        self, ws: WorkspaceClient, object_id: str, acl: list[iam.AccessControlRequest], request_type: RequestObjectType
    ):
        ws.permissions.update(request_type, object_id, acl)

    @sleep_and_retry
    @limits(calls=100, period=1)
    def _crawler_task(
        self,
        ws: WorkspaceClient,
        object_id: str,
        crawler: str,
        request_type: RequestObjectType,
        extras: dict | None = None,
    ) -> PermissionsInventoryItem | None:
        permissions = self._safe_get_permissions(ws, request_type, object_id)
        if permissions:
            return PermissionsInventoryItem(
                object_id=object_id,
                crawler=crawler,
                raw_object_permissions=json.dumps(permissions.as_dict()),
                raw_extras=json.dumps(extras),
            )


class PermissionsSupport(BaseSupport, PermissionsOp):
    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        new_acl = self._prepare_new_acl(
            iam.ObjectPermissions.from_dict(json.loads(item.raw_object_permissions)), migration_state, destination
        )
        return partial(
            self._applier_task, ws=self._ws, request_type=self._request_type, acl=new_acl, object_id=item.object_id
        )

    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        return self._is_item_relevant(item, migration_state)

    def __init__(
        self, listing_function: Callable, id_attribute: str, ws: WorkspaceClient, request_type: RequestObjectType
    ):
        super().__init__(ws)
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._request_type = request_type

    def get_crawler_tasks(self):
        return [
            partial(
                self._crawler_task,
                ws=self._ws,
                object_id=getattr(_object, self._id_attribute),
                request_type=self._request_type,
                crawler=str(self._request_type),
            )
            for _object in self._listing_function()
        ]


class WorkspaceSupport(BaseSupport, PermissionsOp):
    """
    For this class we're using `extras` payload to properly identify the object type we're working with.
    Since all these objects are under `workspace` crawler name, we need to distinct between various request types
    Note that this class heavily shares the code with PermissionsSupport.
    We can't use direct inheritance from PermissionsSupport here due  to different logic of request_type handling.
    Therefore, common methods are put into `PermissionsOp` mixin.
    """

    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        request_type = self.__convert_object_type_to_request_type(item.extras().get("object_type"))
        new_acl = self._prepare_new_acl(
            iam.ObjectPermissions.from_dict(json.loads(item.raw_object_permissions)), migration_state, destination
        )
        return partial(
            self._applier_task, ws=self._ws, request_type=request_type, acl=new_acl, object_id=item.object_id
        )

    def __init__(self, ws: WorkspaceClient, num_threads=20, start_path: str | None = "/"):
        super().__init__(ws)
        self.listing = WorkspaceListing(
            ws,
            num_threads=num_threads,
            with_directories=False,
        )
        self._start_path = start_path

    @staticmethod
    def __convert_object_type_to_request_type(_object: workspace.ObjectInfo) -> RequestObjectType | None:
        match _object.object_type:
            case workspace.ObjectType.NOTEBOOK:
                return RequestObjectType.NOTEBOOKS
            case workspace.ObjectType.DIRECTORY:
                return RequestObjectType.DIRECTORIES
            case workspace.ObjectType.LIBRARY:
                return None
            case workspace.ObjectType.REPO:
                return RequestObjectType.REPOS
            case workspace.ObjectType.FILE:
                return RequestObjectType.FILES
            # silent handler for experiments - they'll be inventorized by the experiments manager
            case None:
                return None

    def get_crawler_tasks(self) -> list[Callable[..., PermissionsInventoryItem | None]]:
        object_infos = self.listing.walk(self._start_path)
        return [
            partial(
                self._crawler_task,
                ws=self._ws,
                object_id=_object.object_id,
                request_type=self.__convert_object_type_to_request_type(_object),
                crawler="workspace",
                extras={"object_type": _object.object_type},
            )
            for _object in object_infos
        ]

    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        return self._is_item_relevant(item, migration_state)
