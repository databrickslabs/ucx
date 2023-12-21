import collections
import functools
import logging
from datetime import timedelta
from functools import partial

import databricks.sdk.core
import pytest
from databricks.sdk import AccountClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import TableInfo

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403
from databricks.labs.ucx.workspace_access.groups import MigratedGroup

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


retry_on_not_found = functools.partial(retried, on=[NotFound], timeout=timedelta(minutes=5))
long_retry_on_not_found = functools.partial(retry_on_not_found, timeout=timedelta(minutes=15))


@pytest.fixture  # type: ignore[no-redef]
def debug_env_name():
    return "ucws"


def get_workspace_membership(ws, resource_type: str = "WorkspaceGroup"):
    membership = collections.defaultdict(set)
    for g in ws.groups.list(attributes="id,displayName,meta,members"):
        if g.display_name in ["users", "admins", "account users"]:
            continue
        if g.meta.resource_type != resource_type:
            continue
        if g.members is None:
            continue
        for m in g.members:
            membership[g.display_name].add(m.display)
    return membership


def account_host(self: databricks.sdk.core.Config) -> str:
    if self.is_azure:
        return "https://accounts.azuredatabricks.net"
    elif self.is_gcp:
        return "https://accounts.gcp.databricks.com/"
    else:
        return "https://accounts.cloud.databricks.com"


@pytest.fixture(scope="session")  # type: ignore[no-redef]
def product_info():
    from databricks.labs.ucx.__about__ import __version__

    return "ucx", __version__


@pytest.fixture  # type: ignore[no-redef]
def acc(ws) -> AccountClient:
    # TODO: https://github.com/databricks/databricks-sdk-py/pull/390
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
    def __init__(self, sql_backend: SqlBackend, schema: str, tables: list[TableInfo]):
        super().__init__(sql_backend, schema)
        self._tables = [
            Table(
                catalog=_.catalog_name,
                database=_.schema_name,
                name=_.name,
                object_type=f"{_.table_type.value}",
                view_text=_.view_definition,
                location=_.storage_location,
                table_format=f"{ _.data_source_format.value}" if _.table_type.value != "VIEW" else None,  # type: ignore[arg-type]
            )
            for _ in tables
        ]

    def snapshot(self) -> list[Table]:
        return self._tables
