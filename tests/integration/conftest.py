import io
import json
import logging
import os
import random
from functools import partial

import databricks.sdk.core
import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import Config, DatabricksError
from databricks.sdk.service.compute import CreatePolicyResponse
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
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
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from databricks.labs.ucx.config import InventoryTable
from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.mixins.fixtures import *  # noqa: F403
from databricks.labs.ucx.providers.mixins.sql import StatementExecutionExt
from databricks.labs.ucx.utils import ThreadedExecution

from .utils import (
    EnvironmentInfo,
    InstanceProfile,
    WorkspaceObjects,
    _set_random_permissions,
)

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

NUM_TEST_GROUPS = int(os.environ.get("NUM_TEST_GROUPS", 5))
NUM_TEST_INSTANCE_PROFILES = int(os.environ.get("NUM_TEST_INSTANCE_PROFILES", 3))
NUM_TEST_CLUSTERS = int(os.environ.get("NUM_TEST_CLUSTERS", 3))
NUM_TEST_CLUSTER_POLICIES = int(os.environ.get("NUM_TEST_CLUSTER_POLICIES", 3))
NUM_TEST_PIPELINES = int(os.environ.get("NUM_TEST_PIPELINES", 3))
NUM_TEST_EXPERIMENTS = int(os.environ.get("NUM_TEST_EXPERIMENTS", 3))
NUM_TEST_MODELS = int(os.environ.get("NUM_TEST_MODELS", 3))
NUM_TEST_WAREHOUSES = int(os.environ.get("NUM_TEST_WAREHOUSES", 3))
NUM_TEST_TOKENS = int(os.environ.get("NUM_TEST_TOKENS", 3))

NUM_THREADS = int(os.environ.get("NUM_TEST_THREADS", 20))
UCX_TESTING_PREFIX = os.environ.get("UCX_TESTING_PREFIX", "ucx")
Threader = partial(ThreadedExecution, num_threads=NUM_THREADS)
load_debug_env_if_runs_from_ide("ucws")  # noqa: F405


def account_host(self: databricks.sdk.core.Config) -> str:
    if self.is_azure:
        return "https://accounts.azuredatabricks.net"
    elif self.is_gcp:
        return "https://accounts.gcp.databricks.com/"
    else:
        return "https://accounts.cloud.databricks.com"


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


@pytest.fixture
def sql_exec(ws: WorkspaceClient):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    statement_execution = StatementExecutionExt(ws.api_client)
    return partial(statement_execution.execute, warehouse_id)


@pytest.fixture
def sql_fetch_all(ws: WorkspaceClient):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    statement_execution = StatementExecutionExt(ws.api_client)
    return partial(statement_execution.execute_fetch_all, warehouse_id)


@pytest.fixture
def make_catalog(sql_exec, make_random):
    cleanup = []

    def inner():
        name = f"ucx_C{make_random(4)}".lower()
        sql_exec(f"CREATE CATALOG {name}")
        cleanup.append(name)
        return name

    yield inner
    logger.debug(f"clearing {len(cleanup)} catalogs")
    for name in cleanup:
        logger.debug(f"removing {name} catalog")
        sql_exec(f"DROP CATALOG IF EXISTS {name} CASCADE")
    logger.debug(f"removed {len(cleanup)} catalogs")


def test_catalog_fixture(make_catalog):
    logger.info(f"Created new catalog: {make_catalog()}")
    logger.info(f"Created new catalog: {make_catalog()}")


@pytest.fixture
def make_schema(sql_exec, make_random):
    cleanup = []

    def inner(catalog="hive_metastore"):
        name = f"{catalog}.ucx_S{make_random(4)}".lower()
        sql_exec(f"CREATE SCHEMA {name}")
        cleanup.append(name)
        return name

    yield inner
    logger.debug(f"clearing {len(cleanup)} schemas")
    for name in cleanup:
        logger.debug(f"removing {name} schema")
        sql_exec(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    logger.debug(f"removed {len(cleanup)} schemas")


def test_schema_fixture(make_schema):
    logger.info(f"Created new schema: {make_schema()}")
    logger.info(f"Created new schema: {make_schema()}")


@pytest.fixture
def make_table(sql_exec, make_schema, make_random):
    cleanup = []

    def inner(
        *,
        catalog="hive_metastore",
        schema: str | None = None,
        ctas: str | None = None,
        non_detla: bool = False,
        external: bool = False,
        view: bool = False,
    ):
        if schema is None:
            schema = make_schema(catalog=catalog)
        name = f"{schema}.ucx_T{make_random(4)}".lower()
        ddl = f'CREATE {"VIEW" if view else "TABLE"} {name}'
        if ctas is not None:
            # temporary (if not view)
            ddl = f"{ddl} AS {ctas}"
        elif non_detla:
            location = "dbfs:/databricks-datasets/iot-stream/data-device"
            ddl = f"{ddl} USING json LOCATION '{location}'"
        elif external:
            # external table
            location = "dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled"
            ddl = f"{ddl} USING delta LOCATION '{location}'"
        else:
            # managed table
            ddl = f"{ddl} (id INT, value STRING)"
        sql_exec(ddl)
        cleanup.append(name)
        return name

    yield inner

    logger.debug(f"clearing {len(cleanup)} tables")
    for name in cleanup:
        logger.debug(f"removing {name} table")
        try:
            sql_exec(f"DROP TABLE IF EXISTS {name}")
        except RuntimeError as e:
            if "Cannot drop a view" in str(e):
                sql_exec(f"DROP VIEW IF EXISTS {name}")
            else:
                raise e
    logger.debug(f"removed {len(cleanup)} tables")


def test_table_fixture(make_table):
    logger.info(f"Created new managed table in new schema: {make_table()}")
    logger.info(f'Created new managed table in default schema: {make_table(schema="default")}')
    logger.info(f"Created new external table in new schema: {make_table(external=True)}")
    logger.info(f"Created new external JSON table in new schema: {make_table(non_detla=True)}")
    logger.info(f'Created new tmp table in new schema: {make_table(ctas="SELECT 2+2 AS four")}')
    logger.info(f'Created new view in new schema: {make_table(view=True, ctas="SELECT 2+2 AS four")}')


@pytest.fixture
def user_pool(ws):
    return list(ws.users.list(filter="displayName sw 'test-user-'", attributes="id, userName, displayName"))


@pytest.fixture
def make_ucx_group(make_random, make_group, make_acc_group, user_pool):
    def inner():
        display_name = f"ucx_{make_random(4)}"
        members = [_.id for _ in random.choices(user_pool, k=random.randint(1, 40))]
        ws_group = make_group(display_name=display_name, members=members, entitlements=["allow-cluster-create"])
        acc_group = make_acc_group(display_name=display_name, members=members)
        return ws_group, acc_group

    return inner


@pytest.fixture
def env(make_ucx_group, make_random) -> EnvironmentInfo:
    test_uid = f"ucx_{make_random(4)}"
    yield EnvironmentInfo(test_uid=test_uid, groups=[make_ucx_group()])


@pytest.fixture
def instance_profiles(env: EnvironmentInfo, ws: WorkspaceClient) -> list[InstanceProfile]:
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


@pytest.fixture
def pipelines(env: EnvironmentInfo, ws: WorkspaceClient) -> list[CreatePipelineResponse]:
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


@pytest.fixture
def cluster_policies(env: EnvironmentInfo, ws: WorkspaceClient) -> list[CreatePolicyResponse]:
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


@pytest.fixture
def experiments(ws: WorkspaceClient, env: EnvironmentInfo) -> list[CreateExperimentResponse]:
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


@pytest.fixture
def models(ws: WorkspaceClient, env: EnvironmentInfo) -> list[ModelDatabricks]:
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


@pytest.fixture
def warehouses(ws: WorkspaceClient, env: EnvironmentInfo) -> list[GetWarehouseResponse]:
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


@pytest.fixture
def tokens(ws: WorkspaceClient, env: EnvironmentInfo) -> list[AccessControlRequest]:
    logger.debug("Adding token-level permissions to groups")

    token_permissions = [
        AccessControlRequest(group_name=ws_group.display_name, permission_level=PermissionLevel.CAN_USE)
        for ws_group, _ in random.sample(env.groups, k=min(len(env.groups), NUM_TEST_TOKENS))
    ]

    ws.permissions.update(
        request_object_type=RequestObjectType.AUTHORIZATION,
        request_object_id="tokens",
        access_control_list=token_permissions,
    )

    yield token_permissions


@pytest.fixture
def workspace_objects(ws: WorkspaceClient, env: EnvironmentInfo) -> WorkspaceObjects:
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


@pytest.fixture
def verifiable_objects(
    cluster_policies,
    pipelines,
    experiments,
    models,
    warehouses,
    tokens,
    workspace_objects,
) -> list[tuple[list, str, RequestObjectType | None]]:
    _verifiable_objects = [
        (workspace_objects, "workspace_objects", None),
        (tokens, "tokens", RequestObjectType.AUTHORIZATION),
        (cluster_policies, "policy_id", RequestObjectType.CLUSTER_POLICIES),
        (pipelines, "pipeline_id", RequestObjectType.PIPELINES),
        (experiments, "experiment_id", RequestObjectType.EXPERIMENTS),
        (models, "id", RequestObjectType.REGISTERED_MODELS),
        (warehouses, "id", RequestObjectType.SQL_WAREHOUSES),
    ]
    yield _verifiable_objects


@pytest.fixture()
def inventory_table(env: EnvironmentInfo, ws: WorkspaceClient, make_catalog, make_schema) -> InventoryTable:
    catalog, schema = make_schema(make_catalog()).split(".")
    table = InventoryTable(
        catalog=catalog,
        database=schema,
        name=f"test_inventory_{env.test_uid}",
    )

    yield table

    logger.debug(f"Cleaning up inventory table {table}")
    try:
        ws.tables.delete(table.to_spark())
        logger.debug(f"Inventory table {table} deleted")
    except Exception as e:
        logger.warning(f"Cannot delete inventory table, skipping it. Original exception {e}")
