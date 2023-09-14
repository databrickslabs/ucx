import json
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam, ml, workspace

from databricks.labs.ucx.mixins.hardening import rate_limited

from databricks.labs.ucx.workspace_access.base import Applier, Crawler, Destination, Permissions, RequestObjectType
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = logging.getLogger(__name__)


@dataclass
class GenericPermissionsInfo:
    object_id: str
    request_type: RequestObjectType


class GenericPermissionsSupport(Crawler, Applier):
    def __init__(self, ws: WorkspaceClient, listings: list[Callable[..., Iterator[GenericPermissionsInfo]]]):
        self._listings = listings
        self._ws = ws

    def is_item_relevant(self, item: Permissions, migration_state: GroupMigrationState) -> bool:
        # passwords and tokens are represented on the workspace-level
        if item.object_id in ("tokens", "passwords"):
            return True
        else:
            mentioned_groups = [
                acl.group_name
                for acl in iam.ObjectPermissions.from_dict(json.loads(item.raw_object_permissions)).access_control_list
            ]
            return any(g in mentioned_groups for g in [info.workspace.display_name for info in migration_state.groups])

    def get_crawler_tasks(self):
        for listing in self._listings:
            for info in listing():
                yield partial(
                    self._crawler_task,
                    ws=self._ws,
                    object_id=info.object_id,
                    request_type=info.request_type,
                )

    def _get_apply_task(
        self, item: Permissions, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        new_acl = self._prepare_new_acl(
            iam.ObjectPermissions.from_dict(json.loads(item.raw_object_permissions)), migration_state, destination
        )

        request_type = (
            RequestObjectType.AUTHORIZATION
            if item.object_type in ("passwords", "tokens")
            else RequestObjectType(item.object_type)
        )

        return partial(
            self._applier_task,
            ws=self._ws,
            request_type=request_type,
            acl=new_acl,
            object_id=item.object_id,
        )

    def _safe_get_permissions(
        self, ws: WorkspaceClient, request_object_type: RequestObjectType, object_id: str
    ) -> iam.ObjectPermissions | None:
        try:
            permissions = ws.permissions.get(request_object_type, object_id)
            return permissions
        except DatabricksError as e:
            if e.error_code in [
                "RESOURCE_DOES_NOT_EXIST",
                "RESOURCE_NOT_FOUND",
                "PERMISSION_DENIED",
                "FEATURE_DISABLED",
            ]:
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

    @rate_limited(max_requests=30)
    def _applier_task(
        self, ws: WorkspaceClient, object_id: str, acl: list[iam.AccessControlRequest], request_type: RequestObjectType
    ):
        ws.permissions.update(request_object_type=request_type, request_object_id=object_id, access_control_list=acl)

    @rate_limited(max_requests=100)
    def _crawler_task(
        self,
        ws: WorkspaceClient,
        object_id: str,
        request_type: RequestObjectType,
    ) -> Permissions | None:
        permissions = self._safe_get_permissions(ws, request_type, object_id)

        support = object_id if request_type == RequestObjectType.AUTHORIZATION else request_type.value

        if permissions:
            return Permissions(
                object_id=object_id,
                object_type=support,
                raw_object_permissions=json.dumps(permissions.as_dict()),
            )


def listing_wrapper(
    func: Callable[..., list], id_attribute: str, object_type: RequestObjectType
) -> Callable[..., Iterator[GenericPermissionsInfo]]:
    def wrapper() -> Iterator[GenericPermissionsInfo]:
        for item in func():
            yield GenericPermissionsInfo(
                object_id=getattr(item, id_attribute),
                request_type=object_type,
            )

    return wrapper


def workspace_listing(ws: WorkspaceClient, num_threads=20, start_path: str | None = "/"):
    def _convert_object_type_to_request_type(_object: workspace.ObjectInfo) -> RequestObjectType | None:
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

    def inner():
        from databricks.labs.ucx.workspace_access.listing import WorkspaceListing

        ws_listing = WorkspaceListing(
            ws,
            num_threads=num_threads,
            with_directories=False,
        )
        for _object in ws_listing.walk(start_path):
            request_type = _convert_object_type_to_request_type(_object)
            if request_type:
                yield GenericPermissionsInfo(object_id=str(_object.object_id), request_type=request_type)

    return inner


def models_listing(ws: WorkspaceClient):
    def inner() -> Iterator[ml.ModelDatabricks]:
        for model in ws.model_registry.list_models():
            model_with_id = ws.model_registry.get_model(model.name).registered_model_databricks
            yield model_with_id

    return inner


def experiments_listing(ws: WorkspaceClient):
    def inner() -> Iterator[ml.Experiment]:
        for experiment in ws.experiments.list_experiments():
            """
            We filter-out notebook-based experiments, because they are covered by notebooks listing
            """
            # workspace-based notebook experiment
            if experiment.tags:
                nb_tag = [t for t in experiment.tags if t.key == "mlflow.experimentType" and t.value == "NOTEBOOK"]
                # repo-based notebook experiment
                repo_nb_tag = [
                    t for t in experiment.tags if t.key == "mlflow.experiment.sourceType" and t.value == "REPO_NOTEBOOK"
                ]
                if nb_tag or repo_nb_tag:
                    continue

            yield experiment

    return inner


def authorization_listing():
    def inner():
        for _value in ["passwords", "tokens"]:
            yield GenericPermissionsInfo(
                object_id=_value,
                request_type=RequestObjectType.AUTHORIZATION,
            )

    return inner
