import io
import json
import logging
import os
import pathlib
import string
import sys
from typing import BinaryIO

import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, iam, jobs, pipelines, workspace

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
