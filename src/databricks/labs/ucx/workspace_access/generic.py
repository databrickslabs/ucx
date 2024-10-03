import datetime
import json
import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from functools import partial

from databricks.labs.blueprint.limiter import rate_limited
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.lsql.backends import SqlBackend
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

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions, StaticListing
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
        try:
            for item in self._func():
                yield GenericPermissionsInfo(getattr(item, self._id_attribute), self._object_type)
        except (NotFound, InternalError) as e:
            logger.warning(f"Listing {self._object_type} failed", exc_info=e)
        since = datetime.datetime.now() - started
        logger.info(f"Listed {self._object_type} in {since}")

    def __repr__(self):
        return f"Listing({self._object_type} via {self._func.__qualname__})"


class GenericPermissionsSupport(AclSupport):
    def __init__(
        self,
        ws: WorkspaceClient,
        listings: list[Listing],
        verify_timeout: timedelta | None = timedelta(minutes=1),
        # this parameter is for testing scenarios only - [{object_type}:{object_id}]
        # it will use StaticListing class to return only object ids that has the same object type
        include_object_permissions: list[str] | None = None,
    ):
        self._ws = ws
        self._listings = listings
        self._verify_timeout = verify_timeout
        self._include_object_permissions = include_object_permissions

    def get_crawler_tasks(self):
        if self._include_object_permissions:
            for item in StaticListing(self._include_object_permissions, self.object_types()):
                yield partial(self._crawler_task, item.object_type, item.object_id)
            return
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
        if item.object_id in {"tokens", "passwords"}:
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
        acls: list[iam.AccessControlResponse] | None = None,
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

    @rate_limited(max_requests=100)
    def _verify(self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]):
        # in-flight check for the applied permissions
        # the api might be inconsistent, therefore we need to check that the permissions were applied
        remote_permission = self._safe_get_permissions(object_type, object_id)
        if remote_permission:
            remote_permission_as_request = self._response_to_request(remote_permission.access_control_list)
            if all(elem in remote_permission_as_request for elem in acl):
                return True
            msg = (
                f"Couldn't find permission for object type {object_type} with id {object_id}\n"
                f"acl to be applied={acl}\n"
                f"acl found in the object={remote_permission_as_request}\n"
            )
            raise NotFound(msg)
        return False

    def get_verify_task(self, item: Permissions) -> Callable[[], bool]:
        acl = iam.ObjectPermissions.from_dict(json.loads(item.raw))
        if not acl.access_control_list:
            raise ValueError(
                f"Access control list not present for object type " f"{item.object_type} and object id {item.object_id}"
            )
        permissions_as_request = self._response_to_request(acl.access_control_list)
        return partial(self._verify, item.object_type, item.object_id, permissions_as_request)

    @rate_limited(max_requests=30)
    def _applier_task(self, object_type: str, object_id: str, acl: list[iam.AccessControlRequest]):
        retryable_exceptions = [InternalError, NotFound, ResourceConflict, TemporarilyUnavailable, DeadlineExceeded]

        update_retry_on_value_error = retried(
            on=retryable_exceptions, timeout=self._verify_timeout  # type: ignore[arg-type]
        )
        update_retried_check = update_retry_on_value_error(self._safe_update_permissions)
        update_retried_check(object_type, object_id, acl)

        retry_on_value_error = retried(on=retryable_exceptions, timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._verify)
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
        for access_control in loaded.access_control_list:
            if not access_control.all_permissions:
                continue
            for permission in access_control.all_permissions:
                if permission.inherited:
                    continue
                acl.append(
                    iam.AccessControlRequest(
                        permission_level=permission.permission_level,
                        service_principal_name=access_control.service_principal_name,
                        group_name=access_control.group_name,
                        user_name=access_control.user_name,
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
            for permission in _item.all_permissions:
                if permission.inherited:
                    continue
                acl_requests.append(
                    iam.AccessControlRequest(
                        group_name=new_group_name,
                        service_principal_name=_item.service_principal_name,
                        user_name=_item.user_name,
                        permission_level=permission.permission_level,
                    )
                )
        return acl_requests

    def __repr__(self):
        return f"GenericPermissionsSupport({self._listings})"


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
        # pylint: disable-next=import-outside-toplevel,redefined-outer-name
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

    def _try_fetch(self) -> Iterable[WorkspaceObjectInfo]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
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

    def __repr__(self):
        return f"WorkspaceListing(start_path={self._start_path})"


def models_listing(ws: WorkspaceClient, num_threads: int | None) -> Callable[[], Iterator[ml.ModelDatabricks]]:
    def inner() -> Iterator[ml.ModelDatabricks]:
        tasks = []
        for model in ws.model_registry.list_models():
            tasks.append(partial(ws.model_registry.get_model, name=model.name))
        models, errors = Threads.gather("listing model ids", tasks, num_threads)
        if len(errors) > 0:
            raise ManyError(errors)
        for model_response in models:
            if not model_response.registered_model_databricks:
                continue
            yield model_response.registered_model_databricks

    return inner


def experiments_listing(ws: WorkspaceClient):
    def _get_repo_nb_tag(experiment):
        repo_nb_tag = []
        for tag in experiment.tags:
            if tag.key == "mlflow.experiment.sourceType" and tag.value == "REPO_NOTEBOOK":
                repo_nb_tag.append(tag)
        return repo_nb_tag

    def inner() -> Iterator[ml.Experiment]:
        for experiment in ws.experiments.list_experiments():
            # We filter-out notebook-based experiments, because they are covered by notebooks listing in
            # workspace-based notebook experiment
            if experiment.tags:
                nb_tag = [t for t in experiment.tags if t.key == "mlflow.experimentType" and t.value == "NOTEBOOK"]
                # repo-based notebook experiment
                repo_nb_tag = _get_repo_nb_tag(experiment)
                if nb_tag or repo_nb_tag:
                    continue

            yield experiment

    return inner


def feature_store_listing(ws: WorkspaceClient):
    def inner() -> list[GenericPermissionsInfo]:
        feature_tables = []
        token = None
        while True:
            result = ws.api_client.do(
                "GET", "/api/2.0/feature-store/feature-tables/search", query={"page_token": token, "max_results": 200}
            )
            assert isinstance(result, dict)
            for table in result.get("feature_tables", []):
                feature_tables.append(GenericPermissionsInfo(table["id"], "feature-tables"))

            if "next_page_token" not in result:
                break
            token = result["next_page_token"]  # type: ignore[index]

        return feature_tables

    return inner


def feature_tables_root_page():
    return [GenericPermissionsInfo("/root", "feature-tables")]


def models_root_page():
    return [GenericPermissionsInfo("/root", "registered-models")]


def tokens_and_passwords():
    for _value in ("tokens", "passwords"):
        yield GenericPermissionsInfo(_value, "authorization")
