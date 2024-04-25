import io
import json
import logging
import os
import pathlib
import shutil
import string
import subprocess
import sys
from collections.abc import Callable, Generator, MutableMapping
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO

import pytest
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import NotFound, ResourceConflict
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, iam, jobs, pipelines, sql, workspace
from databricks.sdk.service._internal import Wait
from databricks.sdk.service.catalog import (
    AwsIamRoleRequest,
    AzureServicePrincipal,
    CatalogInfo,
    DataSourceFormat,
    FunctionInfo,
    SchemaInfo,
    StorageCredentialInfo,
    TableInfo,
    TableType,
)
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedModelInput,
    ServedModelInputWorkloadSize,
    ServingEndpointDetailed,
)
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    GetResponse,
    ObjectTypePlural,
    Query,
    QueryInfo,
)
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks.labs.ucx.workspace_access.groups import MigratedGroup

# this file will get to databricks-labs-pytester project and be maintained/refactored there
# pylint: disable=redefined-outer-name,too-many-try-statements,import-outside-toplevel,unnecessary-lambda,too-complex,invalid-name

logger = logging.getLogger(__name__)


def factory(name, create, remove):
    cleanup = []

    def inner(**kwargs):
        x = create(**kwargs)
        logger.debug(f"added {name} fixture: {x}")
        cleanup.append(x)
        return x

    yield inner
    logger.debug(f"clearing {len(cleanup)} {name} fixtures")
    for x in cleanup:
        try:
            logger.debug(f"removing {name} fixture: {x}")
            remove(x)
        except DatabricksError as e:
            # TODO: fix on the databricks-labs-pytester level
            logger.debug(f"ignoring error while {name} {x} teardown: {e}")


@pytest.fixture
def fresh_wheel_file(tmp_path) -> Path:
    this_file = Path(__file__)
    project_root = this_file.parent.parent.parent.parent.parent.parent.absolute()
    # TODO: we can dynamically determine this with python -m build .
    wheel_name = "databricks_labs_ucx"

    build_root = tmp_path / fresh_wheel_file.__name__
    shutil.copytree(project_root, build_root)
    try:
        completed_process = subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "."],
            capture_output=True,
            cwd=build_root,
            check=True,
        )
        if completed_process.returncode != 0:
            raise RuntimeError(completed_process.stderr)

        found_wheels = list(build_root.glob(f"{wheel_name}-*.whl"))
        if not found_wheels:
            msg = f"cannot find {wheel_name}-*.whl"
            raise RuntimeError(msg)
        if len(found_wheels) > 1:
            conflicts = ", ".join(str(whl) for whl in found_wheels)
            msg = f"more than one wheel match: {conflicts}"
            raise RuntimeError(msg)
        wheel_file = found_wheels[0]

        return wheel_file
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr) from None


@pytest.fixture
def wsfs_wheel(ws, fresh_wheel_file, make_random):
    my_user = ws.current_user.me().user_name
    workspace_location = f"/Users/{my_user}/wheels/{make_random(10)}"
    ws.workspace.mkdirs(workspace_location)

    wsfs_wheel = f"{workspace_location}/{fresh_wheel_file.name}"
    with fresh_wheel_file.open("rb") as f:
        ws.workspace.upload(wsfs_wheel, f, format=ImportFormat.AUTO)

    yield wsfs_wheel

    ws.workspace.delete(workspace_location, recursive=True)


@pytest.fixture
def make_random():
    import random

    def inner(k=16) -> str:
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=int(k)))

    return inner


@pytest.fixture
def product_info():
    return None, None


@pytest.fixture
def ws(product_info, debug_env) -> WorkspaceClient:
    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    product_name, product_version = product_info
    return WorkspaceClient(host=debug_env["DATABRICKS_HOST"], product=product_name, product_version=product_version)


@pytest.fixture
def acc(product_info, debug_env) -> AccountClient:
    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    product_name, product_version = product_info
    logger.debug(f"Running with {len(debug_env)} env variables")
    return AccountClient(
        host=debug_env["DATABRICKS_HOST"],
        account_id=debug_env["DATABRICKS_ACCOUNT_ID"],
        product=product_name,
        product_version=product_version,
    )


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
            [
                PermissionLevel.CAN_VIEW,
                PermissionLevel.CAN_RUN,
                PermissionLevel.CAN_MANAGE,
                PermissionLevel.IS_OWNER,  # cannot be a group
            ],
            _simple,
        ),
        (
            "job",
            "jobs",
            [
                PermissionLevel.CAN_VIEW,
                PermissionLevel.CAN_MANAGE_RUN,
                PermissionLevel.CAN_MANAGE,
                PermissionLevel.IS_OWNER,  # cannot be a group
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
        ("authorization", "authorization", [PermissionLevel.CAN_USE], _simple),
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
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_QUERY],
            _simple,
        ),
        (
            "feature_table",
            "feature-tables",
            [PermissionLevel.CAN_VIEW_METADATA, PermissionLevel.CAN_EDIT_METADATA, PermissionLevel.CAN_MANAGE],
            _simple,
        ),
    ]


def _redash_permissions_mapping():
    def _simple(_, object_id):
        return object_id

    return [
        (
            "query",
            ObjectTypePlural.QUERIES,
            [
                sql.PermissionLevel.CAN_VIEW,
                sql.PermissionLevel.CAN_RUN,
                sql.PermissionLevel.CAN_MANAGE,
                sql.PermissionLevel.CAN_EDIT,
            ],
            _simple,
        ),
        (
            "alert",
            ObjectTypePlural.ALERTS,
            [
                sql.PermissionLevel.CAN_VIEW,
                sql.PermissionLevel.CAN_RUN,
                sql.PermissionLevel.CAN_MANAGE,
                sql.PermissionLevel.CAN_EDIT,
            ],
            _simple,
        ),
        (
            "dashboard",
            ObjectTypePlural.DASHBOARDS,
            [
                sql.PermissionLevel.CAN_VIEW,
                sql.PermissionLevel.CAN_RUN,
                sql.PermissionLevel.CAN_MANAGE,
                sql.PermissionLevel.CAN_EDIT,
            ],
            _simple,
        ),
    ]


class _PermissionsChange:
    def __init__(self, object_id: str, before: list[iam.AccessControlRequest], after: list[iam.AccessControlRequest]):
        self.object_id = object_id
        self.before = before
        self.after = after

    @staticmethod
    def _principal(acr: iam.AccessControlRequest) -> str:
        if acr.user_name is not None:
            return f"user_name {acr.user_name}"
        if acr.group_name is not None:
            return f"group_name {acr.group_name}"
        return f"service_principal_name {acr.service_principal_name}"

    def _list(self, acl: list[iam.AccessControlRequest]):
        return ", ".join(f"{self._principal(_)} {_.permission_level.value}" for _ in acl)

    def __repr__(self):
        return f"{self.object_id} [{self._list(self.before)}] -> [{self._list(self.after)}]"


class _RedashPermissionsChange:
    def __init__(self, object_id: str, before: list[sql.AccessControl], after: list[sql.AccessControl]):
        self.object_id = object_id
        self.before = before
        self.after = after

    @staticmethod
    def _principal(acr: sql.AccessControl) -> str:
        if acr.user_name is not None:
            return f"user_name {acr.user_name}"
        return f"group_name {acr.group_name}"

    def _list(self, acl: list[sql.AccessControl]):
        return ", ".join(f"{self._principal(_)} {_.permission_level.value}" for _ in acl)

    def __repr__(self):
        return f"{self.object_id} [{self._list(self.before)}] -> [{self._list(self.after)}]"


def _make_permissions_factory(name, resource_type, levels, id_retriever):
    def _non_inherited(x: iam.ObjectPermissions):
        out: list[iam.AccessControlRequest] = []
        assert x.access_control_list is not None
        for access_control in x.access_control_list:
            if not access_control.all_permissions:
                continue
            for permission in access_control.all_permissions:
                if not permission.inherited:
                    continue
                out.append(
                    iam.AccessControlRequest(
                        permission_level=permission.permission_level,
                        group_name=access_control.group_name,
                        user_name=access_control.user_name,
                        service_principal_name=access_control.service_principal_name,
                    )
                )
        return out

    def _make_permissions(ws):
        def create(
            *,
            object_id: str,
            permission_level: iam.PermissionLevel | None = None,
            group_name: str | None = None,
            user_name: str | None = None,
            service_principal_name: str | None = None,
            access_control_list: list[iam.AccessControlRequest] | None = None,
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
                    assert permission_level is not None
                    names = ", ".join(_.value for _ in levels)
                    msg = f"invalid permission level: {permission_level.value}. Valid levels: {names}"
                    raise ValueError(msg)

                access_control_list = []
                if group_name is not None:
                    access_control_list.append(
                        iam.AccessControlRequest(
                            group_name=group_name,
                            permission_level=permission_level,
                        )
                    )
                if user_name is not None:
                    access_control_list.append(
                        iam.AccessControlRequest(
                            user_name=user_name,
                            permission_level=permission_level,
                        )
                    )
                if service_principal_name is not None:
                    access_control_list.append(
                        iam.AccessControlRequest(
                            service_principal_name=service_principal_name,
                            permission_level=permission_level,
                        )
                    )
            ws.permissions.update(resource_type, object_id, access_control_list=access_control_list)
            return _PermissionsChange(object_id, initial, access_control_list)

        def remove(change: _PermissionsChange):
            ws.permissions.set(resource_type, change.object_id, access_control_list=change.before)

        yield from factory(f"{name} permissions", create, remove)

    return _make_permissions


def _make_redash_permissions_factory(name, resource_type, levels, id_retriever):
    def _non_inherited(x: GetResponse):
        out: list[sql.AccessControl] = []
        assert x.access_control_list is not None
        for access_control in x.access_control_list:
            out.append(
                sql.AccessControl(
                    permission_level=access_control.permission_level,
                    group_name=access_control.group_name,
                    user_name=access_control.user_name,
                )
            )
        return out

    def _make_permissions(ws):
        def create(
            *,
            object_id: str,
            permission_level: sql.PermissionLevel | None = None,
            group_name: str | None = None,
            user_name: str | None = None,
            access_control_list: list[sql.AccessControl] | None = None,
        ):
            nothing_specified = permission_level is None and access_control_list is None
            both_specified = permission_level is not None and access_control_list is not None
            if nothing_specified or both_specified:
                msg = "either permission_level or access_control_list has to be specified"
                raise ValueError(msg)

            object_id = id_retriever(ws, object_id)
            initial = _non_inherited(ws.dbsql_permissions.get(resource_type, object_id))

            if access_control_list is None:
                if permission_level not in levels:
                    assert permission_level is not None
                    names = ", ".join(_.value for _ in levels)
                    msg = f"invalid permission level: {permission_level.value}. Valid levels: {names}"
                    raise ValueError(msg)

                access_control_list = []
                if group_name is not None:
                    access_control_list.append(
                        sql.AccessControl(
                            group_name=group_name,
                            permission_level=permission_level,
                        )
                    )
                if user_name is not None:
                    access_control_list.append(
                        sql.AccessControl(
                            user_name=user_name,
                            permission_level=permission_level,
                        )
                    )

            ws.dbsql_permissions.set(resource_type, object_id, access_control_list=access_control_list)
            return _RedashPermissionsChange(object_id, initial, access_control_list)

        def remove(change: _RedashPermissionsChange):
            ws.dbsql_permissions.set(
                sql.ObjectTypePlural(resource_type), change.object_id, access_control_list=change.before
            )

        yield from factory(f"{name} permissions", create, remove)

    return _make_permissions


for name, resource_type, levels, id_retriever in _permissions_mapping():
    # wrap function factory, otherwise loop scope sticks the wrong way
    locals()[f"make_{name}_permissions"] = pytest.fixture(
        _make_permissions_factory(name, resource_type, levels, id_retriever)
    )

for name, resource_type, levels, id_retriever in _redash_permissions_mapping():
    # wrap function factory, otherwise loop scope sticks the wrong way
    locals()[f"make_{name}_permissions"] = pytest.fixture(
        _make_redash_permissions_factory(name, resource_type, levels, id_retriever)
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
    def create(
        *,
        path: str | None = None,
        content: BinaryIO | None = None,
        language: Language = Language.PYTHON,
        format: ImportFormat = ImportFormat.SOURCE,  # pylint:  disable=redefined-builtin
        overwrite: bool = False,
    ) -> str:
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
        if content is None:
            content = io.BytesIO(b"print(1)")
        path = str(path)
        ws.workspace.upload(path, content, language=language, format=format, overwrite=overwrite)
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


def _make_group(name, cfg, interface, make_random):
    @retried(on=[ResourceConflict], timeout=timedelta(seconds=30))
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
        # TODO: REQUEST_LIMIT_EXCEEDED: GetUserPermissionsRequest RPC token bucket limit has been exceeded.
        group = interface.create(**kwargs)
        if cfg.is_account_client:
            logger.info(f"Account group {group.display_name}: {cfg.host}/users/groups/{group.id}/members")
        else:
            logger.info(f"Workspace group {group.display_name}: {cfg.host}#setting/accounts/groups/{group.id}")
        return group

    yield from factory(name, create, lambda item: interface.delete(item.id))


@pytest.fixture
def make_group(ws, make_random):
    yield from _make_group("workspace group", ws.config, ws.groups, make_random)


@pytest.fixture
def make_acc_group(acc, make_random):
    yield from _make_group("account group", acc.config, acc.groups, make_random)


@pytest.fixture
def migrated_group(acc, ws, make_group, make_acc_group):
    """Create a pair of groups in workspace and account. Assign account group to workspace."""
    ws_group = make_group()
    acc_group = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), acc_group.id, [iam.WorkspacePermission.USER])
    return MigratedGroup.partial_info(ws_group, acc_group)


@pytest.fixture
def make_cluster_policy(ws, make_random):
    def create(*, name: str | None = None, **kwargs):
        if name is None:
            name = f"sdk-{make_random(4)}"
        if "definition" not in kwargs:
            kwargs["definition"] = json.dumps(
                {
                    "spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": "true"},
                }
            )
        cluster_policy = ws.cluster_policies.create(name, **kwargs)
        logger.info(
            f"Cluster policy: {ws.config.host}#setting/clusters/cluster-policies/view/{cluster_policy.policy_id}"
        )
        return cluster_policy

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
            if "spark_conf" in kwargs:
                kwargs["spark_conf"] = kwargs["spark_conf"] | {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": "local[*]",
                }
            else:
                kwargs["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
            kwargs["custom_tags"] = {"ResourceClass": "SingleNode"}
        if "instance_pool_id" not in kwargs:
            kwargs["node_type_id"] = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)

        return ws.clusters.create(
            cluster_name=cluster_name,
            spark_version=spark_version,
            autotermination_minutes=autotermination_minutes,
            **kwargs,
        )

    yield from factory("cluster", create, lambda item: ws.clusters.permanent_delete(item.cluster_id))


@pytest.fixture
def make_experiment(ws, make_random):
    def create(
        *,
        path: str | None = None,
        experiment_name: str | None = None,
        **kwargs,
    ):
        if path is None:
            path = f"/Users/{ws.current_user.me().user_name}/{make_random(4)}"
        if experiment_name is None:
            experiment_name = f"sdk-{make_random(4)}"

        try:
            ws.workspace.mkdirs(path)
        except DatabricksError:
            pass

        return ws.experiments.create_experiment(name=f"{path}/{experiment_name}", **kwargs)

    yield from factory("experiment", create, lambda item: ws.experiments.delete_experiment(item.experiment_id))


@pytest.fixture
def make_instance_pool(ws, make_random):
    def create(*, instance_pool_name=None, node_type_id=None, **kwargs):
        if instance_pool_name is None:
            instance_pool_name = f"sdk-{make_random(4)}"
        if node_type_id is None:
            node_type_id = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
        return ws.instance_pools.create(instance_pool_name, node_type_id, **kwargs)

    yield from factory("instance pool", create, lambda item: ws.instance_pools.delete(item.instance_pool_id))


@pytest.fixture
def make_job(ws, make_random, make_notebook):
    def create(**kwargs):
        task_spark_conf = None
        if "name" not in kwargs:
            kwargs["name"] = f"sdk-{make_random(4)}"
        if "spark_conf" in kwargs:
            task_spark_conf = kwargs["spark_conf"]
            kwargs.pop("spark_conf")
        if "tasks" not in kwargs:
            if task_spark_conf:
                kwargs["tasks"] = [
                    jobs.Task(
                        task_key=make_random(4),
                        description=make_random(4),
                        new_cluster=compute.ClusterSpec(
                            num_workers=1,
                            node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
                            spark_version=ws.clusters.select_spark_version(latest=True),
                            spark_conf=task_spark_conf,
                        ),
                        notebook_task=jobs.NotebookTask(notebook_path=make_notebook()),
                        timeout_seconds=0,
                    )
                ]
            else:
                kwargs["tasks"] = [
                    jobs.Task(
                        task_key=make_random(4),
                        description=make_random(4),
                        new_cluster=compute.ClusterSpec(
                            num_workers=1,
                            node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
                            spark_version=ws.clusters.select_spark_version(latest=True),
                        ),
                        notebook_task=jobs.NotebookTask(notebook_path=make_notebook()),
                        timeout_seconds=0,
                    )
                ]
        job = ws.jobs.create(**kwargs)
        logger.info(f"Job: {ws.config.host}#job/{job.job_id}")
        return job

    yield from factory("job", create, lambda item: ws.jobs.delete(item.job_id))


@pytest.fixture
def make_model(ws, make_random):
    def create(
        *,
        model_name: str | None = None,
        **kwargs,
    ):
        if model_name is None:
            model_name = f"sdk-{make_random(4)}"

        created_model = ws.model_registry.create_model(model_name, **kwargs)
        model = ws.model_registry.get_model(created_model.registered_model.name)
        return model.registered_model_databricks

    yield from factory("model", create, lambda item: ws.model_registry.delete_model(item.id))


@pytest.fixture
def make_pipeline(ws, make_random, make_notebook):
    def create(**kwargs) -> pipelines.CreatePipelineResponse:
        if "name" not in kwargs:
            kwargs["name"] = f"sdk-{make_random(4)}"
        if "libraries" not in kwargs:
            kwargs["libraries"] = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=make_notebook()))]
        if "clusters" not in kwargs:
            kwargs["clusters"] = [
                pipelines.PipelineCluster(
                    node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
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
            warehouse_type = CreateWarehouseRequestWarehouseType.PRO
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

    yield from factory("warehouse", create, lambda item: ws.warehouses.delete(item.id))


def _is_in_debug() -> bool:
    return os.path.basename(sys.argv[0]) in {"_jb_pytest_runner.py", "testlauncher.py"}


@pytest.fixture
def debug_env_name():
    # Alternatively, we could use @pytest.mark.xxx, but
    # not sure how reusable it becomes then.
    #
    # we don't use scope=session, as monkeypatch.setenv
    # doesn't work on a session level
    return "UNKNOWN"


@pytest.fixture
def debug_env(monkeypatch, debug_env_name) -> MutableMapping[str, str]:
    if not _is_in_debug():
        return os.environ
    conf_file = pathlib.Path.home() / ".databricks/debug-env.json"
    if not conf_file.exists():
        return os.environ
    with conf_file.open("r") as f:
        conf = json.load(f)
        if debug_env_name not in conf:
            sys.stderr.write(f"""{debug_env_name} not found in ~/.databricks/debug-env.json""")
            msg = f"{debug_env_name} not found in ~/.databricks/debug-env.json"
            raise KeyError(msg)
        for k, v in conf[debug_env_name].items():
            monkeypatch.setenv(k, v)
    return os.environ


@pytest.fixture
def env_or_skip(debug_env) -> Callable[[str], str]:
    skip = pytest.skip
    if _is_in_debug():
        skip = pytest.fail  # type: ignore[assignment]

    def inner(var: str) -> str:
        if var not in debug_env:
            skip(f"Environment variable {var} is missing")
        return debug_env[var]

    return inner


@pytest.fixture
def sql_backend(ws, env_or_skip) -> StatementExecutionBackend:
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    return StatementExecutionBackend(ws, warehouse_id)


@pytest.fixture
def inventory_schema(make_schema):
    return make_schema(catalog_name="hive_metastore").name


@pytest.fixture
def make_catalog(ws, sql_backend, make_random) -> Generator[Callable[..., CatalogInfo], None, None]:
    def create() -> CatalogInfo:
        name = f"ucx_C{make_random(4)}".lower()
        sql_backend.execute(f"CREATE CATALOG {name}")
        catalog_info = ws.catalogs.get(name)
        return catalog_info

    yield from factory(
        "catalog",
        create,
        lambda catalog_info: ws.catalogs.delete(catalog_info.full_name, force=True),
    )


@pytest.fixture
def make_schema(ws, sql_backend, make_random) -> Generator[Callable[..., SchemaInfo], None, None]:
    def create(*, catalog_name: str = "hive_metastore", name: str | None = None) -> SchemaInfo:
        if name is None:
            name = f"ucx_S{make_random(4)}".lower()
        full_name = f"{catalog_name}.{name}".lower()
        sql_backend.execute(f"CREATE SCHEMA {full_name}")
        schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name)
        logger.info(
            f"Schema {schema_info.full_name}: "
            f"{ws.config.host}/explore/data/{schema_info.catalog_name}/{schema_info.name}"
        )
        return schema_info

    def remove(schema_info: SchemaInfo):
        try:
            sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_info.full_name} CASCADE")
        except RuntimeError as e:
            if "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("schema", create, remove)


@pytest.fixture
# pylint: disable-next=too-many-statements
def make_table(ws, sql_backend, make_schema, make_random) -> Generator[Callable[..., TableInfo], None, None]:
    def create(  # pylint: disable=too-many-locals,too-many-arguments
        *,
        catalog_name="hive_metastore",
        schema_name: str | None = None,
        name: str | None = None,
        ctas: str | None = None,
        non_delta: bool = False,
        external: bool = False,
        external_csv: str | None = None,
        view: bool = False,
        tbl_properties: dict[str, str] | None = None,
        hiveserde_ddl: str | None = None,
        storage_override: str | None = None,
    ) -> TableInfo:
        if schema_name is None:
            schema = make_schema(catalog_name=catalog_name)
            catalog_name = schema.catalog_name
            schema_name = schema.name
        if name is None:
            name = f"ucx_T{make_random(4)}".lower()
        table_type: TableType | None = None
        data_source_format = None
        storage_location = None
        view_text = None
        full_name = f"{catalog_name}.{schema_name}.{name}".lower()
        ddl = f'CREATE {"VIEW" if view else "TABLE"} {full_name}'
        if view:
            table_type = TableType.VIEW
            view_text = ctas
        if ctas is not None:
            # temporary (if not view)
            ddl = f"{ddl} AS {ctas}"
        elif non_delta:
            table_type = TableType.EXTERNAL  # pylint: disable=redefined-variable-type
            data_source_format = DataSourceFormat.JSON
            storage_location = f"dbfs:/tmp/ucx_test_{make_random(4)}"
            # Modified, otherwise it will identify the table as a DB Dataset
            ddl = (
                f"{ddl} USING json location '{storage_location}' as SELECT * FROM "
                f"JSON.`dbfs:/databricks-datasets/iot-stream/data-device`"
            )
        elif external_csv is not None:
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.CSV
            storage_location = external_csv
            ddl = f"{ddl} USING CSV OPTIONS (header=true) LOCATION '{storage_location}'"
        elif external:
            # external table
            table_type = TableType.EXTERNAL
            data_source_format = DataSourceFormat.DELTASHARING
            url = "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share"
            storage_location = f"{url}#delta_sharing.default.lending_club"
            ddl = f"{ddl} USING deltaSharing LOCATION '{storage_location}'"
        else:
            # managed table
            table_type = TableType.MANAGED
            data_source_format = DataSourceFormat.DELTA
            storage_location = f"dbfs:/user/hive/warehouse/{schema_name}/{name}"
            ddl = f"{ddl} (id INT, value STRING)"
        if tbl_properties:
            str_properties = ",".join([f" '{k}' = '{v}' " for k, v in tbl_properties.items()])
            ddl = f"{ddl} TBLPROPERTIES ({str_properties})"

        if hiveserde_ddl:
            ddl = hiveserde_ddl
            data_source_format = None
            table_type = TableType.EXTERNAL
            storage_location = storage_override

        sql_backend.execute(ddl)
        table_info = TableInfo(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=name,
            full_name=full_name,
            properties=tbl_properties,
            storage_location=storage_location,
            table_type=table_type,
            view_definition=view_text,
            data_source_format=data_source_format,
        )
        logger.info(
            f"Table {table_info.full_name}: "
            f"{ws.config.host}/explore/data/{table_info.catalog_name}/{table_info.schema_name}/{table_info.name}"
        )
        return table_info

    def remove(table_info: TableInfo):
        try:
            sql_backend.execute(f"DROP TABLE IF EXISTS {table_info.full_name}")
        except RuntimeError as e:
            if "Cannot drop a view" in str(e):
                sql_backend.execute(f"DROP VIEW IF EXISTS {table_info.full_name}")
            elif "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("table", create, remove)


@pytest.fixture
def make_udf(sql_backend, make_schema, make_random) -> Generator[Callable[..., FunctionInfo], None, None]:
    def create(
        *, catalog_name="hive_metastore", schema_name: str | None = None, name: str | None = None
    ) -> FunctionInfo:
        if schema_name is None:
            schema = make_schema(catalog_name=catalog_name)
            catalog_name = schema.catalog_name
            schema_name = schema.name

        if name is None:
            name = f"ucx_T{make_random(4)}".lower()

        full_name = f"{catalog_name}.{schema_name}.{name}".lower()
        ddl = f"CREATE FUNCTION {full_name}(x INT) RETURNS FLOAT CONTAINS SQL DETERMINISTIC RETURN 0;"

        sql_backend.execute(ddl)
        udf_info = FunctionInfo(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=name,
            full_name=full_name,
        )

        logger.info(f"Function {udf_info.full_name} crated")
        return udf_info

    def remove(udf_info: FunctionInfo):
        try:
            sql_backend.execute(f"DROP FUNCTION IF EXISTS {udf_info.full_name}")
        except NotFound as e:
            if "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("table", create, remove)


@pytest.fixture
def make_query(ws, make_table, make_random):
    def create() -> QueryInfo:
        table = make_table()
        query_name = f"ucx_query_Q{make_random(4)}"
        query = ws.queries.create(
            name=f"{query_name}",
            description="TEST QUERY FOR UCX",
            query=f"SELECT * FROM {table.schema_name}.{table.name}",
        )
        logger.info(f"Query Created {query_name}: {ws.config.host}/sql/editor/{query.id}")
        return query

    def remove(query: Query):
        try:
            ws.queries.delete(query_id=query.id)
        except RuntimeError as e:
            logger.info(f"Can't drop query {e}")

    yield from factory("query", create, remove)


@pytest.fixture
def make_storage_credential(ws):
    def create(
        *,
        credential_name: str,
        application_id: str = "",
        client_secret: str = "",
        directory_id: str = "",
        aws_iam_role_arn: str = "",
        read_only=False,
    ) -> StorageCredentialInfo:
        if aws_iam_role_arn != "":
            storage_credential = ws.storage_credentials.create(
                credential_name, aws_iam_role=AwsIamRoleRequest(role_arn=aws_iam_role_arn), read_only=read_only
            )
        else:
            azure_service_principal = AzureServicePrincipal(directory_id, application_id, client_secret)
            storage_credential = ws.storage_credentials.create(
                credential_name, azure_service_principal=azure_service_principal, read_only=read_only
            )
        return storage_credential

    def remove(storage_credential: StorageCredentialInfo):
        ws.storage_credentials.delete(storage_credential.name, force=True)

    yield from factory("storage_credential", create, remove)


@pytest.fixture
def make_serving_endpoint(ws, make_random, make_model):
    def create() -> Wait[ServingEndpointDetailed]:
        endpoint_name = make_random(4)
        model = make_model()
        endpoint = ws.serving_endpoints.create(
            endpoint_name,
            EndpointCoreConfigInput(
                served_models=[
                    ServedModelInput(model.name, "1", ServedModelInputWorkloadSize.SMALL, scale_to_zero_enabled=True)
                ]
            ),
        )
        return endpoint

    def remove(endpoint_name: str):
        try:
            ws.serving_endpoints.delete(endpoint_name)
        except RuntimeError as e:
            logger.info(f"Can't remove endpoint {e}")

    yield from factory("Serving endpoint", create, remove)


@pytest.fixture
def make_feature_table(ws, make_random):
    def create():
        feature_table_name = make_random(6) + "." + make_random(6)
        table = ws.api_client.do(
            "POST",
            "/api/2.0/feature-store/feature-tables/create",
            body={"name": feature_table_name, "primary_keys": [{"name": "pk", "data_type": "string"}]},
        )
        return table['feature_table']

    def remove(table: dict):
        try:
            ws.api_client.do("DELETE", "/api/2.0/feature-store/feature-tables/delete", body={"name": table["name"]})
        except RuntimeError as e:
            logger.info(f"Can't remove feature table {e}")

    yield from factory("Feature table", create, remove)


@pytest.fixture
def make_dbfs_data_copy(ws, make_cluster, env_or_skip):
    if ws.config.is_aws:
        cmd_exec = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("TEST_WILDCARD_CLUSTER_ID"))

    def create(*, src_path: str, dst_path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.cp('{src_path}', '{dst_path}', recurse=True)")
        else:
            ws.dbfs.copy(src_path, dst_path, recursive=True)
        return dst_path

    def remove(dst_path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.rm('{dst_path}', recurse=True)")
        else:
            ws.dbfs.delete(dst_path, recursive=True)

    yield from factory("make_dbfs_data_copy", create, remove)


@pytest.fixture
def make_mounted_location(make_random, make_dbfs_data_copy, env_or_skip):
    # make a copy of src data to a new location to avoid overlapping UC table path that will fail other
    # external table migration tests
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    return new_mounted_location


@pytest.fixture
def make_storage_dir(ws, env_or_skip):
    if ws.config.is_aws:
        cmd_exec = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("TEST_WILDCARD_CLUSTER_ID"))

    def create(*, path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.mkdirs('{path}')")
        else:
            ws.dbfs.mkdirs(path)
        return path

    def remove(path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.rm('{path}', recurse=True)")
        else:
            ws.dbfs.delete(path, recursive=True)

    yield from factory("make_storage_dir", create, remove)
