import datetime
import json
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.retries import retried
from databricks.sdk.service import iam, ml

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import (
    AclSupport,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = logging.getLogger(__name__)


@dataclass
class GenericPermissionsInfo:
    object_id: str
    request_type: str


@dataclass
class WorkspaceObjectInfo:
    object_type: str
    object_id: str
    path: str
    language: str = None


class RetryableError(DatabricksError):
    pass


class Listing:
    def __init__(self, func: Callable[..., list], id_attribute: str, object_type: str):
        self._func = func
        self._id_attribute = id_attribute
        self._object_type = object_type

    def object_types(self) -> set[str]:
        return {self._object_type}

    def __iter__(self):
        started = datetime.datetime.now()
        for item in self._func():
            yield GenericPermissionsInfo(getattr(item, self._id_attribute), self._object_type)
        since = datetime.datetime.now() - started
        logger.info(f"Listed {self._object_type} in {since}")


class GenericPermissionsSupport(AclSupport):
    def __init__(
        self, ws: WorkspaceClient, listings: list[Listing], verify_timeout: timedelta | None = timedelta(minutes=20)
    ):
        self._ws = ws
        self._listings = listings
        self._verify_timeout = verify_timeout

    def get_crawler_tasks(self):
        for listing in self._listings:
            for info in listing:
                yield partial(self._crawler_task, info.request_type, info.object_id)

    def object_types(self) -> set[str]:
        all_object_types = set()
        for listing in self._listings:
            for object_type in listing.object_types():
                all_object_types.add(object_type)
        return all_object_types

    def get_apply_task(self, item: Permissions, migration_state: GroupMigrationState, destination: Destination):
        if not self._is_item_relevant(item, migration_state):
            return None
        object_permissions = iam.ObjectPermissions.from_dict(json.loads(item.raw))
        new_acl = self._prepare_new_acl(object_permissions, migration_state, destination)
        return partial(self._applier_task, item.object_type, item.object_id, new_acl)

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: GroupMigrationState) -> bool:
        # passwords and tokens are represented on the workspace-level
        if item.object_id in ("tokens", "passwords"):
            return True
        object_permissions = iam.ObjectPermissions.from_dict(json.loads(item.raw))
        for acl in object_permissions.access_control_list:
            if migration_state.is_in_scope(acl.group_name):
                return True
        return False

    @staticmethod
    def _response_to_request(
        acls: Optional["list[iam.AccessControlResponse]"] = None,
    ) -> list[iam.AccessControlRequest]:
        results = []
        for acl in acls:
            for permission in acl.all_permissions:
                results.append(
                    iam.AccessControlRequest(
                        acl.group_name, permission.permission_level, acl.service_principal_name, acl.user_name
                    )
                )
        return results

    def _inflight_check(self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        set_retry_on_value_error = retried(on=[RetryableError], timeout=self._verify_timeout)
        set_retried_check = set_retry_on_value_error(self._safe_get_permissions)
        remote_permission = set_retried_check(object_type, object_id)
        remote_permission_as_request = self._response_to_request(remote_permission.access_control_list)
        if all(elem in remote_permission_as_request for elem in acl):
            return True
        else:
            msg = f"""Couldn't apply appropriate permission for object type {object_type} with id {object_id}
                acl to be applied={acl}
                acl found in the object={remote_permission_as_request}
                """
            raise ValueError(msg)

    @rate_limited(max_requests=30)
    def _applier_task(self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]):
        update_retry_on_value_error = retried(on=[RetryableError], timeout=self._verify_timeout)
        update_retried_check = update_retry_on_value_error(self._safe_update_permissions)
        update_retried_check(object_type, object_id, acl)

        retry_on_value_error = retried(on=[ValueError], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._inflight_check)
        return retried_check(object_id, object_id, acl)

    @rate_limited(max_requests=100)
    def _crawler_task(self, object_type: str, object_id: str) -> Permissions | None:
        permissions = self._safe_get_permissions(object_type, object_id)
        if not permissions:
            return None
        return Permissions(
            object_id=object_id,
            object_type=object_type,
            raw=json.dumps(permissions.as_dict()),
        )

    def _load_as_request(self, object_type: str, object_id: str) -> list[iam.AccessControlRequest]:
        loaded = self._safe_get_permissions(object_type, object_id)
        if loaded is None:
            return []
        acl = []
        for v in loaded.access_control_list:
            for permission in v.all_permissions:
                if permission.inherited:
                    continue
                acl.append(
                    iam.AccessControlRequest(
                        permission_level=permission.permission_level,
                        service_principal_name=v.service_principal_name,
                        group_name=v.group_name,
                        user_name=v.user_name,
                    )
                )
        # sort to return deterministic results
        return sorted(acl, key=lambda v: f"{v.group_name}:{v.user_name}:{v.service_principal_name}")

    def load_as_dict(self, object_type: str, object_id: str) -> dict[str, iam.PermissionLevel]:
        result = {}
        for acl in self._load_as_request(object_type, object_id):
            result[self._key_for_acl_dict(acl)] = acl.permission_level
        return result

    @staticmethod
    def _key_for_acl_dict(acl: iam.AccessControlRequest) -> str:
        if acl.group_name is not None:
            return acl.group_name
        if acl.user_name is not None:
            return acl.user_name
        return acl.service_principal_name

    # TODO remove after ES-892977 is fixed
    def _safe_get_permissions(self, object_type: str, object_id: str) -> iam.ObjectPermissions | None:
        try:
            return self._ws.permissions.get(object_type, object_id)
        except DatabricksError as e:
            if e.error_code in [
                "RESOURCE_DOES_NOT_EXIST",
                "RESOURCE_NOT_FOUND",
                "PERMISSION_DENIED",
                "FEATURE_DISABLED",
            ]:
                logger.warning(f"Could not get permissions for {object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise RetryableError() from e

    def _safe_update_permissions(
        self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]
    ) -> iam.ObjectPermissions | None:
        try:
            return self._ws.permissions.update(object_type, object_id, access_control_list=acl)
        except DatabricksError as e:
            if e.error_code in [
                "BAD_REQUEST",
                "INVALID_PARAMETER_VALUE",
                "UNAUTHORIZED",
                "PERMISSION_DENIED",
                "FEATURE_DISABLED",
                "RESOURCE_DOES_NOT_EXIST",
            ]:
                logger.warning(f"Could not update permissions for {object_type} {object_id} due to {e.error_code}")
                return None
            else:
                raise RetryableError() from e

    def _prepare_new_acl(
        self, permissions: iam.ObjectPermissions, migration_state: GroupMigrationState, destination: Destination
    ) -> list[iam.AccessControlRequest]:
        _acl = permissions.access_control_list
        acl_requests = []
        coord = f"{permissions.object_type}/{permissions.object_id}"
        for _item in _acl:
            if not migration_state.is_in_scope(_item.group_name):
                logger.debug(f"Skipping {_item} for {coord} because it is not in scope")
                continue
            new_group_name = migration_state.get_target_principal(_item.group_name, destination)
            if new_group_name is None:
                logger.debug(f"Skipping {_item.group_name} for {coord} because it has no target principal")
                continue
            for p in _item.all_permissions:
                if p.inherited:
                    continue
                acl_requests.append(
                    iam.AccessControlRequest(
                        group_name=new_group_name,
                        service_principal_name=_item.service_principal_name,
                        user_name=_item.user_name,
                        permission_level=p.permission_level,
                    )
                )
        return acl_requests


class WorkspaceListing(Listing, CrawlerBase):
    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        inventory_database: str,
        num_threads=20,
        start_path: str | None = "/",
    ):
        Listing.__init__(self, ..., ..., ...)
        CrawlerBase.__init__(
            self,
            backend=sql_backend,
            catalog="hive_metastore",
            schema=inventory_database,
            table="workspace_objects",
            klass=WorkspaceObjectInfo,
        )
        self._ws = ws
        self._num_threads = num_threads
        self._start_path = start_path
        self._sql_backend = sql_backend
        self._inventory_database = inventory_database

    def _crawl(self) -> list[WorkspaceObjectInfo]:
        from databricks.labs.ucx.workspace_access.listing import WorkspaceListing

        ws_listing = WorkspaceListing(self._ws, num_threads=self._num_threads, with_directories=False)
        for obj in ws_listing.walk(self._start_path):
            if obj is None or obj.object_type is None:
                continue
            raw = obj.as_dict()
            yield WorkspaceObjectInfo(
                object_type=raw["object_type"],
                object_id=str(raw["object_id"]),
                path=raw["path"],
                language=raw.get("language", None),
            )

    def snapshot(self) -> list[WorkspaceObjectInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[WorkspaceObjectInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield WorkspaceObjectInfo(*row)

    def object_types(self) -> set[str]:
        return {"notebooks", "directories", "repos", "files"}

    @staticmethod
    def _convert_object_type_to_request_type(_object: WorkspaceObjectInfo) -> str | None:
        match _object.object_type:
            case "NOTEBOOK":
                return "notebooks"
            case "DIRECTORY":
                return "directories"
            case "LIBRARY":
                return None
            case "REPO":
                return "repos"
            case "FILE":
                return "files"
            # silent handler for experiments - they'll be inventorized by the experiments manager
            case None:
                return None

    def __iter__(self):
        for _object in self.snapshot():
            request_type = self._convert_object_type_to_request_type(_object)
            if request_type:
                yield GenericPermissionsInfo(object_id=str(_object.object_id), request_type=request_type)


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


def tokens_and_passwords():
    for _value in ["passwords", "tokens"]:
        yield GenericPermissionsInfo(_value, "authorization")
