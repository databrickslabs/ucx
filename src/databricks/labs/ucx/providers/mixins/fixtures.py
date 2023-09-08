import io
import json
import logging
import os
import pathlib
import string
import sys
from typing import BinaryIO, Optional

import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, iam, jobs, pipelines, workspace
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

_LOG = logging.getLogger(__name__)


def factory(name, create, remove):
    cleanup = []

    def inner(**kwargs):
        x = create(**kwargs)
        _LOG.debug(f"added {name} fixture: {x}")
        cleanup.append(x)
        return x

    yield inner
    _LOG.debug(f"clearing {len(cleanup)} {name} fixtures")
    for x in cleanup:
        try:
            _LOG.debug(f"removing {name} fixture: {x}")
            remove(x)
        except DatabricksError as e:
            # TODO: fix on the databricks-labs-pytester level
            _LOG.debug(f"ignoring error while {name} {x} teardown: {e}")


@pytest.fixture
def make_random():
    import random

    def inner(k=16) -> str:
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=int(k)))

    return inner


@pytest.fixture(scope="session")
def ws() -> WorkspaceClient:
    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    return WorkspaceClient()


@pytest.fixture(scope="session")
def acc() -> AccountClient:
    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    return AccountClient()


def _permissions_mapping():
    from databricks.sdk.service.iam import PermissionLevel

    def _simple(_, object_id):
        return object_id

    def _path(ws, path):
        return ws.workspace.get_status(path).object_id

    return [
        ("cluster_policy", "cluster-policies", [PermissionLevel.CAN_USE], _simple),
        (
            "instance_pool",
            "instance-pools",
            [PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
        (
            "cluster",
            "clusters",
            [PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
        (
            "pipeline",
            "pipelines",
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE, PermissionLevel.IS_OWNER],
            _simple,
        ),
        (
            "job",
            "jobs",
            [
                PermissionLevel.CAN_VIEW,
                PermissionLevel.CAN_MANAGE_RUN,
                PermissionLevel.IS_OWNER,
                PermissionLevel.CAN_MANAGE,
            ],
            _simple,
        ),
        (
            "notebook",
            "notebooks",
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_RUN, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE],
            _path,
        ),
        (
            "directory",
            "directories",
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_RUN, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE],
            _path,
        ),
        (
            "workspace_file",
            "files",
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_RUN, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
        (
            "workspace_file_path",
            "files",
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_RUN, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE],
            _path,
        ),
        (
            "repo",
            "repos",
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_RUN, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE],
            _path,
        ),
        ("tokens_authorization", "authorization", [PermissionLevel.CAN_USE], _simple),
        ("passwords_authorization", "authorization", [PermissionLevel.CAN_USE], _simple),
        (
            "warehouse",
            "sql/warehouses",
            [PermissionLevel.CAN_USE, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
        (
            "dashboard",
            "sql/dashboards",
            [PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_VIEW],
            _simple,
        ),
        (
            "alert",
            "sql/alerts",
            [PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_VIEW],
            _simple,
        ),
        (
            "query",
            "sql/queries",
            [PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_VIEW],
            _simple,
        ),
        (
            "experiment",
            "experiments",
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
        (
            "registered_model",
            "registered-models",
            [
                PermissionLevel.CAN_READ,
                PermissionLevel.CAN_EDIT,
                PermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
                PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
                PermissionLevel.CAN_MANAGE,
            ],
            _simple,
        ),
        (
            "serving_endpoint",
            "serving-endpoints",
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
    ]


class _PermissionsChange:
    def __init__(self, object_id: str, before: list[iam.AccessControlRequest], after: list[iam.AccessControlRequest]):
        self._object_id = object_id
        self._before = before
        self._after = after

    @staticmethod
    def _principal(acr: iam.AccessControlRequest) -> str:
        if acr.user_name is not None:
            return f"user_name {acr.user_name}"
        elif acr.group_name is not None:
            return f"group_name {acr.group_name}"
        else:
            return f"service_principal_name {acr.service_principal_name}"

    def _list(self, acl: list[iam.AccessControlRequest]):
        return ", ".join(f"{self._principal(_)} {_.permission_level.value}" for _ in acl)

    def __repr__(self):
        return f"{self._object_id} [{self._list(self._before)}] -> [{self._list(self._after)}]"


def _make_permissions_factory(name, resource_type, levels, id_retriever):
    def _non_inherited(x: iam.ObjectPermissions):
        return [
            iam.AccessControlRequest(
                permission_level=permission.permission_level,
                group_name=access_control.group_name,
                user_name=access_control.user_name,
                service_principal_name=access_control.service_principal_name,
            )
            for access_control in x.access_control_list
            for permission in access_control.all_permissions
            if not permission.inherited
        ]

    def _make_permissions(ws):
        def create(
            *,
            object_id: str,
            permission_level: iam.PermissionLevel | None = None,
            group_name: str | None = None,
            user_name: str | None = None,
            service_principal_name: str | None = None,
            access_control_list: Optional["list[iam.AccessControlRequest]"] = None,
        ):
            nothing_specified = permission_level is None and access_control_list is None
            both_specified = permission_level is not None and access_control_list is not None
            if nothing_specified or both_specified:
                msg = "either permission_level or access_control_list has to be specified"
                raise ValueError(msg)

            object_id = id_retriever(ws, object_id)
            initial = _non_inherited(ws.permissions.get(resource_type, object_id))
            if access_control_list is None:
                if permission_level not in levels:
                    names = ", ".join(_.value for _ in levels)
                    msg = f"invalid permission level: {permission_level.value}. Valid levels: {names}"
                    raise ValueError(msg)
                access_control_list = [
                    iam.AccessControlRequest(
                        group_name=group_name,
                        user_name=user_name,
                        service_principal_name=service_principal_name,
                        permission_level=permission_level,
                    )
                ]
            ws.permissions.set(resource_type, object_id, access_control_list=access_control_list)
            return _PermissionsChange(object_id, initial, access_control_list)

        def remove(change: _PermissionsChange):
            ws.permissions.set(resource_type, change._object_id, access_control_list=change._before)

        yield from factory(f"{name} permissions", create, remove)

    return _make_permissions


for name, resource_type, levels, id_retriever in _permissions_mapping():
    # wrap function factory, otherwise loop scope sticks the wrong way
    locals()[f"make_{name}_permissions"] = pytest.fixture(
        _make_permissions_factory(name, resource_type, levels, id_retriever)
    )


@pytest.fixture
def make_secret_scope(ws, make_random):
    def create(**kwargs):
        name = f"sdk-{make_random(4)}"
        ws.secrets.create_scope(name, **kwargs)
        return name

    yield from factory("secret scope", create, lambda scope: ws.secrets.delete_scope(scope))


@pytest.fixture
def make_secret_scope_acl(ws):
    def create(*, scope: str, principal: str, permission: workspace.AclPermission):
        ws.secrets.put_acl(scope, principal, permission)
        return scope, principal

    yield from factory("secret scope acl", create, lambda x: ws.secrets.delete_acl(x[0], x[1]))


@pytest.fixture
def make_notebook(ws, make_random):
    def create(*, path: str | None = None, content: BinaryIO | None = None, **kwargs):
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}.py"
        if content is None:
            content = io.BytesIO(b"print(1)")
        ws.workspace.upload(path, content, **kwargs)
        return path

    yield from factory("notebook", create, lambda x: ws.workspace.delete(x))


@pytest.fixture
def make_directory(ws, make_random):
    def create(*, path: str | None = None):
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
        ws.workspace.mkdirs(path)
        return path

    yield from factory("directory", create, lambda x: ws.workspace.delete(x, recursive=True))


@pytest.fixture
def make_repo(ws, make_random):
    def create(*, url=None, provider=None, path=None, **kwargs):
        if path is None:
            path = f"/Repos/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
        if url is None:
            url = "https://github.com/shreyas-goenka/empty-repo.git"
        if provider is None:
            provider = "github"
        return ws.repos.create(url, provider, path=path, **kwargs)

    yield from factory("repo", create, lambda x: ws.repos.delete(x.id))


@pytest.fixture
def make_user(ws, make_random):
    yield from factory(
        "workspace user",
        lambda **kwargs: ws.users.create(user_name=f"sdk-{make_random(4)}@example.com".lower(), **kwargs),
        lambda item: ws.users.delete(item.id),
    )


def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


def _make_group(name, interface, make_random):
    def create(
        *,
        members: list[str] | None = None,
        roles: list[str] | None = None,
        entitlements: list[str] | None = None,
        display_name: str | None = None,
        **kwargs,
    ):
        kwargs["display_name"] = f"sdk-{make_random(4)}" if display_name is None else display_name
        if members is not None:
            kwargs["members"] = _scim_values(members)
        if roles is not None:
            kwargs["roles"] = _scim_values(roles)
        if entitlements is not None:
            kwargs["entitlements"] = _scim_values(entitlements)
        return interface.create(**kwargs)

    yield from factory(name, create, lambda item: interface.delete(item.id))


@pytest.fixture
def make_group(ws, make_random):
    yield from _make_group("workspace group", ws.groups, make_random)


@pytest.fixture
def make_acc_group(acc, make_random):
    yield from _make_group("account group", acc.groups, make_random)


@pytest.fixture
def make_cluster_policy(ws, make_random):
    def create(*, name: str | None = None, **kwargs):
        if name is None:
            name = f"sdk-{make_random(4)}"
        if "definition" not in kwargs:
            kwargs["definition"] = json.dumps(
                {"spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": True}}
            )
        return ws.cluster_policies.create(name, **kwargs)

    yield from factory("cluster policy", create, lambda item: ws.cluster_policies.delete(item.policy_id))


@pytest.fixture
def make_cluster(ws, make_random):
    def create(
        *,
        single_node: bool = False,
        cluster_name: str | None = None,
        spark_version: str | None = None,
        autotermination_minutes=10,
        **kwargs,
    ):
        if cluster_name is None:
            cluster_name = f"sdk-{make_random(4)}"
        if spark_version is None:
            spark_version = ws.clusters.select_spark_version(latest=True)
        if single_node:
            kwargs["num_workers"] = 0
            kwargs["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
            kwargs["custom_tags"] = {"ResourceClass": "SingleNode"}
        elif "instance_pool_id" not in kwargs:
            kwargs["node_type_id"] = ws.clusters.select_node_type(local_disk=True)

        return ws.clusters.create(
            cluster_name=cluster_name,
            spark_version=spark_version,
            autotermination_minutes=autotermination_minutes,
            **kwargs,
        )

    yield from factory("cluster", create, lambda item: ws.clusters.permanent_delete(item.cluster_id))


@pytest.fixture
def make_instance_pool(ws, make_random):
    def create(*, instance_pool_name=None, node_type_id=None, **kwargs):
        if instance_pool_name is None:
            instance_pool_name = f"sdk-{make_random(4)}"
        if node_type_id is None:
            node_type_id = ws.clusters.select_node_type(local_disk=True)
        return ws.instance_pools.create(instance_pool_name, node_type_id, **kwargs)

    yield from factory("instance pool", create, lambda item: ws.instance_pools.delete(item.instance_pool_id))


@pytest.fixture
def make_job(ws, make_random, make_notebook):
    def create(**kwargs):
        if "name" not in kwargs:
            kwargs["name"] = f"sdk-{make_random(4)}"
        if "tasks" not in kwargs:
            kwargs["tasks"] = [
                jobs.Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=compute.ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                    ),
                    notebook_task=jobs.NotebookTask(notebook_path=make_notebook()),
                    timeout_seconds=0,
                )
            ]
        return ws.jobs.create(**kwargs)

    yield from factory("job", create, lambda item: ws.jobs.delete(item.job_id))


@pytest.fixture
def make_pipeline(ws, make_random, make_notebook):
    def create(**kwargs):
        if "name" not in kwargs:
            kwargs["name"] = f"sdk-{make_random(4)}"
        if "libraries" not in kwargs:
            kwargs["libraries"] = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=make_notebook()))]
        if "clusters" not in kwargs:
            kwargs["clusters"] = [
                pipelines.PipelineCluster(
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    label="default",
                    num_workers=1,
                    custom_tags={
                        "cluster_type": "default",
                    },
                )
            ]
        return ws.pipelines.create(continuous=False, **kwargs)

    yield from factory("delta live table", create, lambda item: ws.pipelines.delete(item.pipeline_id))


@pytest.fixture
def make_warehouse(ws, make_random):
    def create(
        *,
        warehouse_name: str | None = None,
        warehouse_type: CreateWarehouseRequestWarehouseType | None = None,
        cluster_size: str | None = None,
        max_num_clusters: int = 1,
        enable_serverless_compute: bool = False,
        **kwargs,
    ):
        if warehouse_name is None:
            warehouse_name = f"sdk-{make_random(4)}"
        if warehouse_type is None:
            warehouse_type = (CreateWarehouseRequestWarehouseType.PRO,)
        if cluster_size is None:
            cluster_size = "2X-Small"

        return ws.warehouses.create(
            name=warehouse_name,
            cluster_size=cluster_size,
            warehouse_type=warehouse_type,
            max_num_clusters=max_num_clusters,
            enable_serverless_compute=enable_serverless_compute,
            **kwargs,
        )

    yield from factory("warehouse", create, lambda item: ws.warehouses.delete(item.warehouse_id))


def load_debug_env_if_runs_from_ide(key) -> bool:
    if not _is_in_debug():
        return False
    conf_file = pathlib.Path.home() / ".databricks/debug-env.json"
    with conf_file.open("r") as f:
        conf = json.load(f)
        if key not in conf:
            msg = f"{key} not found in ~/.databricks/debug-env.json"
            raise KeyError(msg)
        for k, v in conf[key].items():
            os.environ[k] = v
    return True


def _is_in_debug() -> bool:
    return os.path.basename(sys.argv[0]) in [
        "_jb_pytest_runner.py",
        "testlauncher.py",
    ]
