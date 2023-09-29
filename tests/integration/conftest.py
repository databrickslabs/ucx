import logging
import os
import random
from functools import partial

import databricks.sdk.core
import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import Config

from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403
from databricks.labs.ucx.mixins.sql import StatementExecutionExt

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

load_debug_env_if_runs_from_ide("ucws")  # noqa: F405


def account_host(self: databricks.sdk.core.Config) -> str:
    if self.is_azure:
        return "https://accounts.azuredatabricks.net"
    elif self.is_gcp:
        return "https://accounts.gcp.databricks.com/"
    else:
        return "https://accounts.cloud.databricks.com"


@pytest.fixture(scope="session")
def product_info():
    from databricks.labs.ucx.__about__ import __version__

    return "ucx", __version__


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
    def create():
        name = f"ucx_C{make_random(4)}".lower()
        sql_exec(f"CREATE CATALOG {name}")
        return name

    yield from factory("catalog", create, lambda name: sql_exec(f"DROP CATALOG IF EXISTS {name} CASCADE"))  # noqa: F405


def test_catalog_fixture(make_catalog):
    logger.info(f"Created new catalog: {make_catalog()}")
    logger.info(f"Created new catalog: {make_catalog()}")


@pytest.fixture
def make_schema(sql_exec, make_random):
    def create(*, catalog: str = "hive_metastore", schema: str = None):
        if schema is None:
            schema = f'ucx_S{make_random(4)}'
        schema = f"{catalog}.{schema}".lower()
        sql_exec(f"CREATE SCHEMA {schema}")
        return schema

    yield from factory(  # noqa: F405
        "schema", create, lambda schema_name: sql_exec(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    )


def test_schema_fixture(make_schema):
    logger.info(f"Created new schema: {make_schema()}")
    logger.info(f"Created new schema: {make_schema()}")


@pytest.fixture
def make_table(sql_exec, make_schema, make_random):
    def create(
        *,
        catalog="hive_metastore",
        schema: str | None = None,
        ctas: str | None = None,
        non_delta: bool = False,
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
        elif non_delta:
            location = "dbfs:/databricks-datasets/iot-stream/data-device"
            ddl = f"{ddl} USING json LOCATION '{location}'"
        elif external:
            # external table
            url = "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share"
            share = f"{url}#delta_sharing.default.lending_club"
            ddl = f"{ddl} USING deltaSharing LOCATION '{share}'"
        else:
            # managed table
            ddl = f"{ddl} (id INT, value STRING)"
        sql_exec(ddl)
        return name

    def remove(name):
        try:
            sql_exec(f"DROP TABLE IF EXISTS {name}")
        except RuntimeError as e:
            if "Cannot drop a view" in str(e):
                sql_exec(f"DROP VIEW IF EXISTS {name}")
            else:
                raise e

    yield from factory("table", create, remove)  # noqa: F405


def test_table_fixture(make_table):
    logger.info(f"Created new managed table in new schema: {make_table()}")
    logger.info(f'Created new managed table in default schema: {make_table(schema="default")}')
    logger.info(f"Created new external table in new schema: {make_table(external=True)}")
    logger.info(f"Created new external JSON table in new schema: {make_table(non_delta=True)}")
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
