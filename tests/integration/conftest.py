import os
import random
import uuid
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import pytest
from _pytest.fixtures import SubRequest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.iam import (
    AccessControlRequest,
    ComplexValue,
    Group,
    PermissionLevel,
    User,
)
from dotenv import load_dotenv

from uc_migration_toolkit.config import (
    AuthConfig,
    InventoryTable,
    RateLimitConfig,
    WorkspaceAuthConfig,
)
from uc_migration_toolkit.managers.inventory.types import RequestObjectType
from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient, provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import (
    Request,
    ThreadedExecution,
    WorkspaceLevelEntitlement,
)


def initialize_env() -> None:
    principal_env = Path(__file__).parent.parent.parent / ".env.principal"

    if principal_env.exists():
        logger.info("Using credentials provided in .env.principal")
        load_dotenv(dotenv_path=principal_env)
    else:
        logger.info(f"No .env.principal found at {principal_env.absolute()}, using environment variables")


initialize_env()

NUM_TEST_GROUPS = os.environ.get("NUM_TEST_GROUPS", 5)
NUM_THREADS = os.environ.get("NUM_TEST_THREADS", 20)
DB_CONNECT_CLUSTER_NAME = os.environ.get("DB_CONNECT_CLUSTER_NAME", "ucx-integration-testing")
UCX_TESTING_PREFIX = os.environ.get("UCX_TESTING_PREFIX", "ucx")

Threader = partial(ThreadedExecution, num_threads=NUM_THREADS, rate_limit=RateLimitConfig())


def generate_group_by_id(
    _ws: WorkspaceClient, _acc: AccountClient, group_name: str, users_sample: list[User]
) -> tuple[Group, Group]:
    entities = [ComplexValue(display=user.display_name, value=user.id) for user in users_sample]
    logger.info(f"Creating group with name {group_name}")

    def get_random_entitlements():
        chosen: list[WorkspaceLevelEntitlement] = random.choices(
            list(WorkspaceLevelEntitlement),
            k=random.randint(1, 3),
        )
        entitlements = [ComplexValue(display=None, primary=None, type=None, value=value) for value in chosen]
        return entitlements

    ws_group = _ws.groups.create(display_name=group_name, members=entities, entitlements=get_random_entitlements())
    acc_group = _acc.groups.create(display_name=group_name, members=entities)
    return ws_group, acc_group


def _create_groups(_ws: ImprovedWorkspaceClient, _acc: AccountClient, prefix: str) -> list[tuple[Group, Group]]:
    logger.info("Listing users to create sample groups")
    test_users = list(_ws.users.list(filter="displayName sw 'test-user-'", attributes="id, userName, displayName"))
    logger.info(f"Total of test users {len(test_users)}")
    user_samples: dict[str, list[User]] = {
        f"{prefix}-test-group-{gid}": random.choices(test_users, k=random.randint(1, 40))
        for gid in range(NUM_TEST_GROUPS)
    }
    executables = [
        partial(generate_group_by_id, _ws, _acc, group_name, users_sample)
        for group_name, users_sample in user_samples.items()
    ]
    return Threader(executables).run()


@pytest.fixture(scope="session")
def ws() -> ImprovedWorkspaceClient:
    auth_config = AuthConfig(
        workspace=WorkspaceAuthConfig(
            host=os.environ["DATABRICKS_WS_HOST"],
            client_id=os.environ["DATABRICKS_COMMON_CLIENT_ID"],
            client_secret=os.environ["DATABRICKS_COMMON_CLIENT_SECRET"],
        )
    )
    provider.set_ws_client(auth_config, pool_size=NUM_THREADS)
    yield provider.ws


@pytest.fixture(scope="session", autouse=True)
def acc() -> AccountClient:
    acc_client = AccountClient(
        host=os.environ["DATABRICKS_ACC_HOST"],
        client_id=os.environ["DATABRICKS_COMMON_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_COMMON_CLIENT_SECRET"],
        account_id=os.environ["DATABRICKS_ACC_ACCOUNT_ID"],
    )
    yield acc_client


@pytest.fixture(scope="session", autouse=True)
def dbconnect(ws: ImprovedWorkspaceClient):
    dbc_cluster = next(filter(lambda c: c.cluster_name == DB_CONNECT_CLUSTER_NAME, ws.clusters.list()), None)

    if dbc_cluster:
        logger.info(f"Integration testing cluster {DB_CONNECT_CLUSTER_NAME} already exists, skipping it's creation")
    else:
        logger.info("Creating a cluster for integration testing")
        request = {
            "cluster_name": DB_CONNECT_CLUSTER_NAME,
            "spark_version": "13.2.x-scala2.12",
            "instance_pool_id": os.environ["TEST_POOL_ID"],
            "driver_instance_pool_id": os.environ["TEST_POOL_ID"],
            "num_workers": 0,
            "spark_conf": {"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"},
            "custom_tags": {
                "ResourceClass": "SingleNode",
            },
            "data_security_mode": "SINGLE_USER",
            "autotermination_minutes": 180,
            "runtime_engine": "PHOTON",
        }

        dbc_cluster = ws.clusters.create(spark_version="13.2.x-scala2.12", request=Request(request))

        logger.info(f"Cluster {dbc_cluster.cluster_id} created")

    os.environ["DATABRICKS_CLUSTER_ID"] = dbc_cluster.cluster_id
    yield


@dataclass
class EnvironmentInfo:
    test_uid: str
    groups: list[tuple[Group, Group]]


@pytest.fixture(scope="session", autouse=True)
def env(ws: ImprovedWorkspaceClient, acc: AccountClient, request: SubRequest) -> EnvironmentInfo:
    # prepare environment
    test_uid = f"{UCX_TESTING_PREFIX}_{str(uuid.uuid4())[:8]}"
    logger.info(f"Creating environment with uid {test_uid}")
    groups = _create_groups(ws, acc, test_uid)

    def _cleanup_groups(_ws: WorkspaceClient, _acc: AccountClient, _groups: tuple[Group, Group]):
        ws_g, acc_g = _groups
        logger.info(f"Deleting groups {ws_g.display_name} [ws-level] and {acc_g.display_name} [acc-level]")
        try:
            ws.groups.delete(ws_g.id)
        except Exception as e:
            logger.warning(f"Cannot delete ws-level group, skipping it. Original exception {e}")
        try:
            g = next(iter(acc.groups.list(filter=f"displayName eq '{acc_g.display_name}'")), None)
            if g:
                acc.groups.delete(g.id)
        except Exception as e:
            logger.warning(f"Cannot delete acc-level group, skipping it. Original exception {e}")

    def post_cleanup():
        print("\n")
        logger.info("Cleaning up the environment")
        logger.info("Deleting test groups")
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
            partial(silent_delete, g.id) for g in ws.groups.list(filter=f"displayName sw 'db-temp-{test_uid}'")
        ]
        new_ws_groups_cleanups = [
            partial(silent_delete, g.id) for g in ws.groups.list(filter=f"displayName sw '{test_uid}'")
        ]

        all_cleanups = cleanups + temp_cleanups + new_ws_groups_cleanups
        Threader(all_cleanups).run()
        logger.info(f"Finished cleanup for the environment {test_uid}")

    request.addfinalizer(post_cleanup)
    yield EnvironmentInfo(test_uid=test_uid, groups=groups)


@pytest.fixture(scope="session", autouse=True)
def clusters(env: EnvironmentInfo, ws: ImprovedWorkspaceClient) -> list[ClusterDetails]:
    logger.info("Creating test clusters")

    test_clusters = [
        ws.clusters.create(
            spark_version="13.2.x-scala2.12",
            instance_pool_id=os.environ["TEST_POOL_ID"],
            driver_instance_pool_id=os.environ["TEST_POOL_ID"],
            cluster_name=f"{env.test_uid}-test-{i}",
            num_workers=1,
        )
        for i in range(3)
    ]

    for cluster in test_clusters:

        def get_random_ws_group() -> Group:
            return random.choice([g[0] for g in env.groups])

        def get_random_permission_level() -> PermissionLevel:
            return random.choice(
                [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_ATTACH_TO]
            )

        acl_req = [
            AccessControlRequest(
                group_name=get_random_ws_group().display_name, permission_level=get_random_permission_level()
            )
            for _ in range(3)
        ]

        ws.permissions.set(
            request_object_type=RequestObjectType.CLUSTERS,
            request_object_id=cluster.cluster_id,
            access_control_list=acl_req,
        )

    yield test_clusters

    logger.info("Deleting test clusters")
    executables = [partial(ws.clusters.permanent_delete, c.cluster_id) for c in test_clusters]
    Threader(executables).run()
    logger.info("Test clusters deleted")


@pytest.fixture()
def inventory_table(env: EnvironmentInfo) -> InventoryTable:
    table = InventoryTable(
        catalog="main",
        database="default",
        name=f"test_inventory_{env.test_uid}",
    )

    yield table

    logger.info(f"Cleaning up inventory table {table}")
    try:
        provider.ws.tables.delete(table.to_spark())
        logger.info(f"Inventory table {table} deleted")
    except Exception as e:
        logger.warning(f"Cannot delete inventory table, skipping it. Original exception {e}")
