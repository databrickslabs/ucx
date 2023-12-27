import datetime
import json
import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    DeadlineExceeded,
    InternalError,
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
    ResourceConflict,
    TemporarilyUnavailable,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import iam, ml
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import ManyError, Threads
from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = logging.getLogger(__name__)


@dataclass
class GenericPermissionsInfo:
    object_id: str
    request_type: str


@dataclass
class WorkspaceObjectInfo:
    path: str
    object_type: str | None = None
    object_id: str | None = None
    language: str | None = None


class Listing:
    def __init__(self, func: Callable[..., Iterable], id_attribute: str, object_type: str):
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
        self, ws: WorkspaceClient, listings: list[Listing], verify_timeout: timedelta | None = timedelta(minutes=1)
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

    def get_apply_task(self, item: Permissions, migration_state: MigrationState):
        if not self._is_item_relevant(item, migration_state):
            return None
        object_permissions = iam.ObjectPermissions.from_dict(json.loads(item.raw))
        new_acl = self._prepare_new_acl(object_permissions, migration_state)
        return partial(self._applier_task, item.object_type, item.object_id, new_acl)

    @staticmethod
    def _is_item_relevant(item: Permissions, migration_state: MigrationState) -> bool:
        # passwords and tokens are represented on the workspace-level
        if item.object_id in ("tokens", "passwords"):
            return True
        object_permissions = iam.ObjectPermissions.from_dict(json.loads(item.raw))
        assert object_permissions.access_control_list is not None
        for acl in object_permissions.access_control_list:
            if not acl.group_name:
                continue
            if migration_state.is_in_scope(acl.group_name):
                return True
        return False

    @staticmethod
    def _response_to_request(
        acls: Optional["list[iam.AccessControlResponse]"] = None,
    ) -> list[iam.AccessControlRequest]:
        results: list[iam.AccessControlRequest] = []
        if not acls:
            return results
        for acl in acls:
            if not acl.all_permissions:
                continue
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
        remote_permission = self._safe_get_permissions(object_type, object_id)
        if remote_permission:
            remote_permission_as_request = self._response_to_request(remote_permission.access_control_list)
            if all(elem in remote_permission_as_request for elem in acl):
                return True
            else:
                msg = f"""Couldn't apply appropriate permission for object type {object_type} with id {object_id}
                    acl to be applied={acl}
                    acl found in the object={remote_permission_as_request}
                    """
                raise ValueError(msg)
        return False

    @rate_limited(max_requests=30)
    def _applier_task(self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]):
        retryable_exceptions = [InternalError, NotFound, ResourceConflict, TemporarilyUnavailable, DeadlineExceeded]

        update_retry_on_value_error = retried(
            on=retryable_exceptions, timeout=self._verify_timeout  # type: ignore[arg-type]
        )
        update_retried_check = update_retry_on_value_error(self._safe_update_permissions)
        update_retried_check(object_type, object_id, acl)

        retry_on_value_error = retried(on=[*retryable_exceptions, ValueError], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._inflight_check)
        return retried_check(object_type, object_id, acl)

    @rate_limited(max_requests=100)
    def _crawler_task(self, object_type: str, object_id: str) -> Permissions | None:
        objects_with_owner_permission = ["jobs", "pipelines"]

        permissions = self._safe_get_permissions(object_type, object_id)
        if not permissions:
            logger.warning(f"Object {object_type} {object_id} doesn't have any permissions")
            return None
        if not self._object_have_owner(permissions) and object_type in objects_with_owner_permission:
            logger.warning(
                f"Object {object_type} {object_id} doesn't have any Owner and cannot be migrated "
                f"to account level groups, consider setting a new owner or deleting this object"
            )
            return None
        return Permissions(
            object_id=object_id,
            object_type=object_type,
            raw=json.dumps(permissions.as_dict()),
        )

    def _object_have_owner(self, permissions: iam.ObjectPermissions | None):
        if not permissions:
            return False
        if not permissions.access_control_list:
            return False
        for acl in permissions.access_control_list:
            if not acl.all_permissions:
                continue
            for perm in acl.all_permissions:
                if perm.permission_level == PermissionLevel.IS_OWNER:
                    return True
        return False

    def _load_as_request(self, object_type: str, object_id: str) -> list[iam.AccessControlRequest]:
        loaded = self._safe_get_permissions(object_type, object_id)
        if loaded is None:
            return []
        acl: list[iam.AccessControlRequest] = []
        if not loaded.access_control_list:
            return acl
        for v in loaded.access_control_list:
            if not v.all_permissions:
                continue
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
            if not acl.permission_level:
                continue
            result[self._key_for_acl_dict(acl)] = acl.permission_level
        return result

    @staticmethod
    def _key_for_acl_dict(acl: iam.AccessControlRequest) -> str:
        if acl.group_name is not None:
            return acl.group_name
        if acl.user_name is not None:
            return acl.user_name
        if acl.service_principal_name is not None:
            return acl.service_principal_name
        return "UNKNOWN"

    # TODO remove after ES-892977 is fixed
    @retried(on=[InternalError], timeout=timedelta(minutes=5))
    def _safe_get_permissions(self, object_type: str, object_id: str) -> iam.ObjectPermissions | None:
        try:
            return self._ws.permissions.get(object_type, object_id)
        except PermissionDenied:
            logger.warning(f"permission denied: {object_type} {object_id}")
            return None
        except NotFound:
            logger.warning(f"removed on backend: {object_type} {object_id}")
            return None
        except InvalidParameterValue:
            logger.warning(f"jobs or cluster removed on backend: {object_type} {object_id}")
            return None

    def _safe_update_permissions(
        self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]
    ) -> iam.ObjectPermissions | None:
        try:
            return self._ws.permissions.update(object_type, object_id, access_control_list=acl)
        except PermissionDenied:
            logger.warning(f"permission denied: {object_type} {object_id}")
            return None
        except NotFound:
            logger.warning(f"removed on backend: {object_type} {object_id}")
            return None
        except InvalidParameterValue:
            logger.warning(f"jobs or cluster removed on backend: {object_type} {object_id}")
            return None

    def _prepare_new_acl(
        self, permissions: iam.ObjectPermissions, migration_state: MigrationState
    ) -> list[iam.AccessControlRequest]:
        _acl = permissions.access_control_list
        acl_requests: list[iam.AccessControlRequest] = []
        if not _acl:
            return acl_requests
        coord = f"{permissions.object_type}/{permissions.object_id}"
        for _item in _acl:
            if not _item.group_name:
                continue
            if not migration_state.is_in_scope(_item.group_name):
                logger.debug(f"Skipping {_item} for {coord} because it is not in scope")
                continue
            new_group_name = migration_state.get_target_principal(_item.group_name)
            if new_group_name is None:
                logger.debug(f"Skipping {_item.group_name} for {coord} because it has no target principal")
                continue
            if not _item.all_permissions:
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


class WorkspaceListing(Listing, CrawlerBase[WorkspaceObjectInfo]):
    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        inventory_database: str,
        num_threads=20,
        start_path: str | None = "/",
    ):
        Listing.__init__(self, lambda: [], "_", "_")
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

    def _crawl(self) -> Iterable[WorkspaceObjectInfo]:
        from databricks.labs.ucx.workspace_access.listing import WorkspaceListing

        ws_listing = WorkspaceListing(self._ws, num_threads=self._num_threads, with_directories=False)
        for obj in ws_listing.walk(self._start_path):
            if obj is None or obj.object_type is None:
                continue
            raw = obj.as_dict()
            yield WorkspaceObjectInfo(
                object_type=raw.get("object_type", None),
                object_id=str(raw.get("object_id", None)),
                path=raw.get("path", None),
                language=raw.get("language", None),
            )

    def snapshot(self) -> Iterable[WorkspaceObjectInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[WorkspaceObjectInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield WorkspaceObjectInfo(
                path=row["path"], object_type=row["object_type"], object_id=row["object_id"], language=row["language"]
            )

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
        # silent handler for experiments - they'll be inventoried by the experiments manager
        return None

    def __iter__(self):
        for _object in self.snapshot():
            request_type = self._convert_object_type_to_request_type(_object)
            if not request_type:
                continue
            assert _object.object_id is not None
            yield GenericPermissionsInfo(str(_object.object_id), request_type)


def models_listing(ws: WorkspaceClient, num_threads: int):
    def inner() -> Iterator[ml.ModelDatabricks]:
        tasks = []
        for m in ws.model_registry.list_models():
            tasks.append(partial(ws.model_registry.get_model, name=m.name))
        models, errors = Threads.gather("listing model ids", tasks, num_threads)
        if len(errors) > 0:
            raise ManyError(errors)
        for model in models:
            if not model.registered_model_databricks:
                continue
            yield model.registered_model_databricks

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
    for _value in ["tokens", "passwords"]:
        yield GenericPermissionsInfo(_value, "authorization")
