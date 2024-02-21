import collections
import functools
import logging
from datetime import timedelta
from functools import partial

import databricks.sdk.core
import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.installation import Installation
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import FunctionInfo, TableInfo

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.udfs import Udf, UdfsCrawler

# pylint: disable-next=unused-wildcard-import,wildcard-import
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403
from databricks.labs.ucx.workspace_access.groups import MigratedGroup

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture  # type: ignore[no-redef]
def debug_env_name():  # pylint: disable=function-redefined
    return "ucws"


def get_workspace_membership(workspace_client, res_type: str = "WorkspaceGroup"):
    membership = collections.defaultdict(set)
    for group in workspace_client.groups.list(attributes="id,displayName,meta,members"):
        if group.display_name in {"users", "admins", "account users"}:
            continue
        if group.meta.resource_type != res_type:
            continue
        if group.members is None:
            continue
        for member in group.members:
            membership[group.display_name].add(member.display)
    return membership


def account_host(self: databricks.sdk.core.Config) -> str:
    if self.is_azure:
        return "https://accounts.azuredatabricks.net"
    if self.is_gcp:
        return "https://accounts.gcp.databricks.com/"
    return "https://accounts.cloud.databricks.com"


@pytest.fixture(scope="session")  # type: ignore[no-redef]
def product_info():  # pylint: disable=function-redefined
    return "ucx", __version__


@pytest.fixture  # type: ignore[no-redef]
def acc(ws) -> AccountClient:  # pylint: disable=function-redefined
    return AccountClient(host=ws.config.environment.deployment_url('accounts'))


@pytest.fixture
def sql_exec(sql_backend):
    return partial(sql_backend.execute)


@pytest.fixture
def sql_fetch_all(sql_backend):
    return partial(sql_backend.fetch)


@pytest.fixture
def make_ucx_group(make_random, make_group, make_acc_group, make_user):
    def inner(workspace_group_name=None, account_group_name=None):
        if not workspace_group_name:
            workspace_group_name = f"ucx_{make_random(4)}"
        if not account_group_name:
            account_group_name = workspace_group_name
        user = make_user()
        members = [user.id]
        ws_group = make_group(display_name=workspace_group_name, members=members, entitlements=["allow-cluster-create"])
        acc_group = make_acc_group(display_name=account_group_name, members=members)
        return ws_group, acc_group

    return inner


@pytest.fixture
def make_group_pair(make_random, make_group):
    def inner() -> MigratedGroup:
        suffix = make_random(4)
        ws_group = make_group(display_name=f"old_{suffix}", entitlements=["allow-cluster-create"])
        acc_group = make_group(display_name=f"new_{suffix}")
        return MigratedGroup.partial_info(ws_group, acc_group)

    return inner


class StaticTablesCrawler(TablesCrawler):
    def __init__(self, sb: SqlBackend, schema: str, tables: list[TableInfo]):
        super().__init__(sb, schema)
        self._tables = [
            Table(
                catalog=_.catalog_name,
                database=_.schema_name,
                name=_.name,
                object_type=f"{_.table_type.value}",
                view_text=_.view_definition,
                location=_.storage_location,
                table_format=f"{_.data_source_format.value}" if _.table_type.value != "VIEW" else None,  # type: ignore[arg-type]
            )
            for _ in tables
        ]

    def snapshot(self) -> list[Table]:
        return self._tables


class StaticUdfsCrawler(UdfsCrawler):
    def __init__(self, sb: SqlBackend, schema: str, udfs: list[FunctionInfo]):
        super().__init__(sb, schema)
        self._udfs = [
            Udf(
                catalog=_.catalog_name,
                database=_.schema_name,
                name=_.name,
                body="5",
                comment="_",
                data_access="CONTAINS SQL",
                deterministic=True,
                func_input="STRING",
                func_returns="INT",
                func_type="SQL",
            )
            for _ in udfs
        ]

    def snapshot(self) -> list[Udf]:
        return self._udfs


class StaticTableMapping(TableMapping):
    def __init__(self, workspace_client: WorkspaceClient, sb: SqlBackend, rules: list[Rule]):
        installation = Installation(workspace_client, 'ucx')
        super().__init__(installation, workspace_client, sb)
        self._rules = rules

    def load(self):
        return self._rules

    def save(self, tables: TablesCrawler, workspace_info: WorkspaceInfo) -> str:
        raise RuntimeWarning("not available")
