import io
import json
import logging
import os
import pathlib
import random
import string
import sys
import uuid
from functools import partial

import databricks.sdk.core
import pytest
from _pytest.fixtures import SubRequest
from databricks.sdk import AccountClient
from databricks.sdk.core import Config, DatabricksError
from databricks.sdk.service.compute import (
    ClusterDetails,
    CreateInstancePoolResponse,
    CreatePolicyResponse,
    DataSecurityMode,
    RuntimeEngine,
)
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.jobs import CreateResponse
from databricks.sdk.service.ml import CreateExperimentResponse, ModelDatabricks
from databricks.sdk.service.ml import PermissionLevel as ModelPermissionLevel
from databricks.sdk.service.pipelines import (
    CreatePipelineResponse,
    NotebookLibrary,
    PipelineLibrary,
)
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    GetWarehouseResponse,
)
from databricks.sdk.service.workspace import (
    AclPermission,
    ObjectInfo,
    ObjectType,
    SecretScope,
)

from databricks.labs.ucx.config import InventoryTable
from databricks.labs.ucx.managers.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.utils import ThreadedExecution

from .utils import (
    EnvironmentInfo,
    InstanceProfile,
    WorkspaceObjects,
    _cleanup_groups,
    _create_groups,
    _get_basic_job_cluster,
    _get_basic_task,
    _set_random_permissions,
)

logging.getLogger("databricks.sdk").setLevel(logging.INFO)

NUM_TEST_GROUPS = int(os.environ.get("NUM_TEST_GROUPS", 5))
NUM_TEST_INSTANCE_PROFILES = int(os.environ.get("NUM_TEST_INSTANCE_PROFILES", 3))
NUM_TEST_CLUSTERS = int(os.environ.get("NUM_TEST_CLUSTERS", 3))
NUM_TEST_INSTANCE_POOLS = int(os.environ.get("NUM_TEST_INSTANCE_POOLS", 3))
NUM_TEST_CLUSTER_POLICIES = int(os.environ.get("NUM_TEST_CLUSTER_POLICIES", 3))
NUM_TEST_PIPELINES = int(os.environ.get("NUM_TEST_PIPELINES", 3))
NUM_TEST_JOBS = int(os.environ.get("NUM_TEST_JOBS", 3))
NUM_TEST_EXPERIMENTS = int(os.environ.get("NUM_TEST_EXPERIMENTS", 3))
NUM_TEST_MODELS = int(os.environ.get("NUM_TEST_MODELS", 3))
NUM_TEST_WAREHOUSES = int(os.environ.get("NUM_TEST_WAREHOUSES", 3))
NUM_TEST_TOKENS = int(os.environ.get("NUM_TEST_TOKENS", 3))
NUM_TEST_SECRET_SCOPES = int(os.environ.get("NUM_TEST_SECRET_SCOPES", 10))

NUM_THREADS = int(os.environ.get("NUM_TEST_THREADS", 20))
DB_CONNECT_CLUSTER_NAME = os.environ.get("DB_CONNECT_CLUSTER_NAME", "ucx-integration-testing")
UCX_TESTING_PREFIX = os.environ.get("UCX_TESTING_PREFIX", "ucx")
Threader = partial(ThreadedExecution, num_threads=NUM_THREADS)


def account_host(self: databricks.sdk.core.Config) -> str:
    if self.is_azure:
        return "https://accounts.azuredatabricks.net"
    elif self.is_gcp:
        return "https://accounts.gcp.databricks.com/"
    else:
        return "https://accounts.cloud.databricks.com"


def _load_debug_env_if_runs_from_ide(key) -> bool:
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


@pytest.fixture
def make_random():
    import random

    def inner(k=16) -> str:
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=int(k)))

    return inner


@pytest.fixture(scope="session")
def ws() -> ImprovedWorkspaceClient:
    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    _load_debug_env_if_runs_from_ide("ucws")
    return ImprovedWorkspaceClient()


@pytest.fixture(scope="session")
def acc(ws) -> AccountClient:
    # TODO: move to SDK
    def account_host(cfg: Config) -> str:
        if cfg.is_azure:
            return "https://accounts.azuredatabricks.net"
        elif cfg.is_gcp:
            return "https://accounts.gcp.databricks.com/"
        else:
            return "https://accounts.cloud.databricks.com"

    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    return AccountClient(host=account_host(ws.config))


@pytest.fixture(scope="session")
def dbconnect_cluster_id(ws: ImprovedWorkspaceClient) -> str:
    # TODO: will use predeclared DATABRICKS_CLUSTER_ID env variable
    dbc_cluster = next(filter(lambda c: c.cluster_name == DB_CONNECT_CLUSTER_NAME, ws.clusters.list()), None)

    if dbc_cluster:
        logger.debug(f"Integration testing cluster {DB_CONNECT_CLUSTER_NAME} already exists, skipping it's creation")
        return dbc_cluster.cluster_id

    logger.debug("Creating a cluster for integration testing")
    dbc_cluster = ws.clusters.create(
        spark_version=ws.clusters.select_spark_version(latest=True),
        cluster_name=DB_CONNECT_CLUSTER_NAME,
        instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
        driver_node_type_id=os.environ["TEST_INSTANCE_POOL_ID"],
        num_workers=0,
        spark_conf={"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"},
        custom_tags={"ResourceClass": "SingleNode"},
        data_security_mode=DataSecurityMode.SINGLE_USER,
        autotermination_minutes=60,
        runtime_engine=RuntimeEngine.PHOTON,
    )
    logger.debug(f"Cluster {dbc_cluster.cluster_id} created")

    # TODO: pre-create the cluster in the test infra
    return dbc_cluster.cluster_id


@pytest.fixture(scope="session")
def env(ws: ImprovedWorkspaceClient, acc: AccountClient, request: SubRequest) -> EnvironmentInfo:
    # prepare environment
    test_uid = f"{UCX_TESTING_PREFIX}_{str(uuid.uuid4())[:8]}"
    logger.debug(f"Creating environment with uid {test_uid}")
    groups = _create_groups(ws, acc, test_uid, NUM_TEST_GROUPS, Threader)

    def post_cleanup():
        print("\n")
        logger.debug("Cleaning up the environment")
        logger.debug("Deleting test groups")
        cleanups = [partial(_cleanup_groups, ws, acc, g) for g in groups]

        def error_silencer(func):
            def _wrapped(*args, **kwargs):
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"Cannot delete temp group, skipping it. Original exception {e}")

            return _wrapped

        silent_delete = error_silencer(ws.groups.delete)

        temp_cleanups = [
            # TODO: this is too heavy for SCIM API, refactor to ID lookup
            partial(silent_delete, g.id)
            for g in ws.groups.list(filter=f"displayName sw 'db-temp-{test_uid}'")
        ]
        new_ws_groups_cleanups = [
            partial(silent_delete, g.id) for g in ws.groups.list(filter=f"displayName sw '{test_uid}'")
        ]

        all_cleanups = cleanups + temp_cleanups + new_ws_groups_cleanups
        Threader(all_cleanups).run()
        logger.debug(f"Finished cleanup for the environment {test_uid}")

    request.addfinalizer(post_cleanup)
    yield EnvironmentInfo(test_uid=test_uid, groups=groups)


@pytest.fixture(scope="session")
def instance_profiles(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[InstanceProfile]:
    logger.debug("Adding test instance profiles")
    profiles: list[InstanceProfile] = []

    for i in range(NUM_TEST_INSTANCE_PROFILES):
        profile_arn = f"arn:aws:iam::123456789:instance-profile/{env.test_uid}-test-{i}"
        iam_role_arn = f"arn:aws:iam::123456789:role/{env.test_uid}-test-{i}"
        ws.instance_profiles.add(instance_profile_arn=profile_arn, iam_role_arn=iam_role_arn, skip_validation=True)
        profiles.append(InstanceProfile(instance_profile_arn=profile_arn, iam_role_arn=iam_role_arn))

    for ws_group, _ in env.groups:
        if random.choice([True, False]):
            # randomize to apply roles randomly
            roles = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                    {
                        "op": "add",
                        "path": "roles",
                        "value": [{"value": p.instance_profile_arn} for p in random.choices(profiles, k=2)],
                    }
                ],
            }
            ws.api_client.do("PATCH", f"/api/2.0/preview/scim/v2/Groups/{ws_group.id}", data=json.dumps(roles))

    yield profiles

    logger.debug("Deleting test instance profiles")
    for profile in profiles:
        ws.instance_profiles.remove(profile.instance_profile_arn)
    logger.debug("Test instance profiles deleted")


@pytest.fixture(scope="session")
def instance_pools(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[CreateInstancePoolResponse]:
    logger.debug("Creating test instance pools")

    test_instance_pools: list[CreateInstancePoolResponse] = [
        ws.instance_pools.create(instance_pool_name=f"{env.test_uid}-test-{i}", node_type_id="i3.xlarge")
        for i in range(NUM_TEST_INSTANCE_POOLS)
    ]

    _set_random_permissions(
        test_instance_pools,
        "instance_pool_id",
        RequestObjectType.INSTANCE_POOLS,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE],
    )

    yield test_instance_pools

    logger.debug("Deleting test instance pools")
    executables = [partial(ws.instance_pools.delete, p.instance_pool_id) for p in test_instance_pools]
    Threader(executables).run()


@pytest.fixture(scope="session")
def pipelines(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[CreatePipelineResponse]:
    logger.debug("Creating test DLT pipelines")

    test_pipelines: list[CreatePipelineResponse] = [
        ws.pipelines.create(
            name=f"{env.test_uid}-test-{i}",
            continuous=False,
            development=True,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path="/Workspace/sample-notebook"))],
        )
        for i in range(NUM_TEST_PIPELINES)
    ]

    _set_random_permissions(
        test_pipelines,
        "pipeline_id",
        RequestObjectType.PIPELINES,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_VIEW, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE],
    )

    yield test_pipelines

    logger.debug("Deleting test instance pools")
    executables = [partial(ws.pipelines.delete, p.pipeline_id) for p in test_pipelines]
    Threader(executables).run()


@pytest.fixture(scope="session")
def jobs(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[CreateResponse]:
    logger.debug("Creating test jobs")

    test_jobs: list[CreateResponse] = [
        ws.jobs.create(
            name=f"{env.test_uid}-test-{i}", job_clusters=[_get_basic_job_cluster()], tasks=[_get_basic_task()]
        )
        for i in range(NUM_TEST_JOBS)
    ]

    _set_random_permissions(
        test_jobs,
        "job_id",
        RequestObjectType.JOBS,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE_RUN, PermissionLevel.CAN_MANAGE],
    )

    yield test_jobs

    logger.debug("Deleting test jobs")
    executables = [partial(ws.jobs.delete, j.job_id) for j in test_jobs]
    Threader(executables).run()


@pytest.fixture(scope="session")
def cluster_policies(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[CreatePolicyResponse]:
    logger.debug("Creating test cluster policies")

    test_cluster_policies: list[CreatePolicyResponse] = [
        ws.cluster_policies.create(
            name=f"{env.test_uid}-test-{i}",
            definition="""
        {
          "spark_version": {
                "type": "unlimited",
                "defaultValue": "auto:latest-lts"
            }
        }
        """,
        )
        for i in range(NUM_TEST_CLUSTER_POLICIES)
    ]

    _set_random_permissions(
        test_cluster_policies,
        "policy_id",
        RequestObjectType.CLUSTER_POLICIES,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_USE],
    )

    yield test_cluster_policies

    logger.debug("Deleting test instance pools")
    executables = [partial(ws.cluster_policies.delete, p.policy_id) for p in test_cluster_policies]
    Threader(executables).run()


@pytest.fixture(scope="session")
def clusters(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[ClusterDetails]:
    logger.debug("Creating test clusters")

    creators = [
        partial(
            ws.clusters.create,
            spark_version=ws.clusters.select_spark_version(latest=True),
            instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
            driver_instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
            cluster_name=f"{env.test_uid}-test-{i}",
            num_workers=1,
        )
        for i in range(NUM_TEST_CLUSTERS)
    ]

    test_clusters = Threader(creators).run()

    _set_random_permissions(
        test_clusters,
        "cluster_id",
        RequestObjectType.CLUSTERS,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART],
    )

    yield test_clusters

    logger.debug("Deleting test clusters")
    executables = [partial(ws.clusters.permanent_delete, c.cluster_id) for c in test_clusters]
    Threader(executables).run()
    logger.debug("Test clusters deleted")


@pytest.fixture(scope="session")
def experiments(ws: ImprovedWorkspaceClient, env: EnvironmentInfo) -> list[CreateExperimentResponse]:
    logger.debug("Creating test experiments")

    try:
        ws.workspace.mkdirs("/experiments")
    except DatabricksError:
        pass

    test_experiments = Threader(
        [
            partial(ws.experiments.create_experiment, name=f"/experiments/{env.test_uid}-test-{i}")
            for i in range(NUM_TEST_EXPERIMENTS)
        ]
    ).run()

    _set_random_permissions(
        test_experiments,
        "experiment_id",
        RequestObjectType.EXPERIMENTS,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_READ, PermissionLevel.CAN_EDIT],
    )

    yield test_experiments

    logger.debug("Deleting test experiments")
    executables = [partial(ws.experiments.delete_experiment, e.experiment_id) for e in test_experiments]
    Threader(executables).run()
    logger.debug("Test experiments deleted")


@pytest.fixture(scope="session")
def models(ws: ImprovedWorkspaceClient, env: EnvironmentInfo) -> list[ModelDatabricks]:
    logger.debug("Creating models")

    test_models: list[ModelDatabricks] = [
        ws.model_registry.get_model(
            ws.model_registry.create_model(f"{env.test_uid}-test-{i}").registered_model.name
        ).registered_model_databricks
        for i in range(NUM_TEST_MODELS)
    ]

    _set_random_permissions(
        test_models,
        "id",
        RequestObjectType.REGISTERED_MODELS,
        env,
        ws,
        permission_levels=[
            ModelPermissionLevel.CAN_READ,
            ModelPermissionLevel.CAN_MANAGE,
            ModelPermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
            ModelPermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
        ],
    )

    yield test_models

    logger.debug("Deleting test models")
    executables = [partial(ws.model_registry.delete_model, m.name) for m in test_models]
    Threader(executables).run()
    logger.debug("Test models deleted")


@pytest.fixture(scope="session")
def warehouses(ws: ImprovedWorkspaceClient, env: EnvironmentInfo) -> list[GetWarehouseResponse]:
    logger.debug("Creating warehouses")

    creators = [
        partial(
            ws.warehouses.create,
            name=f"{env.test_uid}-test-{i}",
            cluster_size="2X-Small",
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            max_num_clusters=1,
            enable_serverless_compute=False,
        )
        for i in range(NUM_TEST_WAREHOUSES)
    ]

    test_warehouses: list[GetWarehouseResponse] = Threader(creators).run()

    _set_random_permissions(
        test_warehouses,
        "id",
        RequestObjectType.SQL_WAREHOUSES,
        env,
        ws,
        permission_levels=[PermissionLevel.CAN_USE, PermissionLevel.CAN_MANAGE],
    )

    yield test_warehouses

    logger.debug("Deleting test warehouses")
    executables = [partial(ws.warehouses.delete, w.id) for w in test_warehouses]
    Threader(executables).run()
    logger.debug("Test warehouses deleted")


@pytest.fixture(scope="session")
def tokens(ws: ImprovedWorkspaceClient, env: EnvironmentInfo) -> list[AccessControlRequest]:
    logger.debug("Adding token-level permissions to groups")

    token_permissions = [
        AccessControlRequest(group_name=ws_group.display_name, permission_level=PermissionLevel.CAN_USE)
        for ws_group, _ in random.sample(env.groups, k=NUM_TEST_TOKENS)
    ]

    ws.permissions.update(
        request_object_type=RequestObjectType.AUTHORIZATION,
        request_object_id="tokens",
        access_control_list=token_permissions,
    )

    yield token_permissions


@pytest.fixture(scope="session")
def secret_scopes(ws: ImprovedWorkspaceClient, env: EnvironmentInfo) -> list[SecretScope]:
    logger.debug("Creating test secret scopes")

    for i in range(NUM_TEST_SECRET_SCOPES):
        ws.secrets.create_scope(f"{env.test_uid}-test-{i}")

    test_secret_scopes = [s for s in ws.secrets.list_scopes() if s.name.startswith(env.test_uid)]

    for scope in test_secret_scopes:
        random_permission = random.choice(list(AclPermission))
        random_ws_group, _ = random.choice(env.groups)
        ws.secrets.put_acl(scope.name, random_ws_group.display_name, random_permission)

    yield test_secret_scopes

    logger.debug("Deleting test secret scopes")
    executables = [partial(ws.secrets.delete_scope, s.name) for s in test_secret_scopes]
    Threader(executables).run()


@pytest.fixture(scope="session")
def workspace_objects(ws: ImprovedWorkspaceClient, env: EnvironmentInfo) -> WorkspaceObjects:
    logger.info(f"Creating test workspace objects under /{env.test_uid}")
    ws.workspace.mkdirs(f"/{env.test_uid}")

    base_dirs = []

    for ws_group, _ in env.groups:
        _path = f"/{env.test_uid}/{ws_group.display_name}"
        ws.workspace.mkdirs(_path)
        object_info = ws.workspace.get_status(_path)
        base_dirs.append(object_info)

        ws.permissions.set(
            request_object_type=RequestObjectType.DIRECTORIES,
            request_object_id=object_info.object_id,
            access_control_list=[
                AccessControlRequest(group_name=ws_group.display_name, permission_level=PermissionLevel.CAN_MANAGE)
            ],
        )

    notebooks = []

    for nb_idx in range(3):
        random_group = random.choice([g[0] for g in env.groups])
        _nb_path = f"/{env.test_uid}/{random_group.display_name}/nb-{nb_idx}.py"
        ws.workspace.upload(path=_nb_path, content=io.BytesIO(b"print(1)"))
        # TODO: add a proper test for this
        # if random.choice([True, False]):
        #     ws.experiments.create_experiment(name=_nb_path)  # create experiment to test nb-based experiments
        _nb_obj = ws.workspace.get_status(_nb_path)
        notebooks.append(_nb_obj)
        ws.permissions.set(
            request_object_type=RequestObjectType.NOTEBOOKS,
            request_object_id=_nb_obj.object_id,
            access_control_list=[
                AccessControlRequest(group_name=random_group.display_name, permission_level=PermissionLevel.CAN_EDIT)
            ],
        )

    yield WorkspaceObjects(
        root_dir=ObjectInfo(
            path=f"/{env.test_uid}",
            object_type=ObjectType.DIRECTORY,
            object_id=ws.workspace.get_status(f"/{env.test_uid}").object_id,
        ),
        directories=base_dirs,
        notebooks=notebooks,
    )

    logger.debug("Deleting test workspace objects")
    ws.workspace.delete(f"/{env.test_uid}", recursive=True)
    logger.debug("Test workspace objects deleted")


@pytest.fixture(scope="session")
def verifiable_objects(
    clusters,
    instance_pools,
    cluster_policies,
    pipelines,
    jobs,
    experiments,
    models,
    warehouses,
    tokens,
    secret_scopes,
    workspace_objects,
) -> list[tuple[list, str, RequestObjectType | None]]:
    _verifiable_objects = [
        (workspace_objects, "workspace_objects", None),
        (secret_scopes, "secret_scopes", None),
        (tokens, "tokens", RequestObjectType.AUTHORIZATION),
        (clusters, "cluster_id", RequestObjectType.CLUSTERS),
        (instance_pools, "instance_pool_id", RequestObjectType.INSTANCE_POOLS),
        (cluster_policies, "policy_id", RequestObjectType.CLUSTER_POLICIES),
        (pipelines, "pipeline_id", RequestObjectType.PIPELINES),
        (jobs, "job_id", RequestObjectType.JOBS),
        (experiments, "experiment_id", RequestObjectType.EXPERIMENTS),
        (models, "id", RequestObjectType.REGISTERED_MODELS),
        (warehouses, "id", RequestObjectType.SQL_WAREHOUSES),
    ]
    yield _verifiable_objects


@pytest.fixture()
def inventory_table(env: EnvironmentInfo, ws: ImprovedWorkspaceClient, dbconnect_cluster_id: str) -> InventoryTable:
    ws.config.cluster_id = dbconnect_cluster_id
    table = InventoryTable(
        catalog="main",
        database="default",
        name=f"test_inventory_{env.test_uid}",
    )

    yield table

    logger.debug(f"Cleaning up inventory table {table}")
    try:
        ws.tables.delete(table.to_spark())
        logger.debug(f"Inventory table {table} deleted")
    except Exception as e:
        logger.warning(f"Cannot delete inventory table, skipping it. Original exception {e}")
