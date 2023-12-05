import collections
import functools
import logging
import random
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

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


retry_on_not_found = functools.partial(retried, on=[NotFound], timeout=timedelta(minutes=5))
long_retry_on_not_found = functools.partial(retry_on_not_found, timeout=timedelta(minutes=15))


@pytest.fixture
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


@pytest.fixture(scope="session")
def product_info():
    from databricks.labs.ucx.__about__ import __version__

    return "ucx", __version__


@pytest.fixture
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
def make_ucx_group_suffix(make_random, make_group, make_acc_group, user_pool):
    assert (
        len(user_pool) >= 1
    ), "must have 'test-user-*' test users with id, userName and displayName in your test workspace"

    def inner(suffix="_PROD"):
        display_name = f"ucx_{make_random(4)}"
        members = [_.id for _ in random.choices(user_pool, k=random.randint(1, 40))]
        ws_group = make_group(display_name=display_name, members=members, entitlements=["allow-cluster-create"])
        acc_group = make_acc_group(display_name=display_name + suffix, members=members)
        return ws_group, acc_group

    return inner


@pytest.fixture
def make_ucx_group_prefix(make_random, make_group, make_acc_group, user_pool):
    assert (
        len(user_pool) >= 1
    ), "must have 'test-user-*' test users with id, userName and displayName in your test workspace"

    def inner(prefix="PROD_"):
        display_name = f"ucx_{make_random(4)}"
        members = [_.id for _ in random.choices(user_pool, k=random.randint(1, 40))]
        ws_group = make_group(display_name=display_name, members=members, entitlements=["allow-cluster-create"])
        acc_group = make_acc_group(display_name=prefix + display_name, members=members)
        return ws_group, acc_group

    return inner


@pytest.fixture
def make_ucx_group_replace(make_random, make_group, make_acc_group, user_pool):
    assert (
        len(user_pool) >= 1
    ), "must have 'test-user-*' test users with id, userName and displayName in your test workspace"

    def inner(ws_elem="DS", acct_elem="DataScience"):
        rand = make_random()
        ws_group_name = f"ucx_{ws_elem}_{rand}"
        acct_group_name = f"ucx_{acct_elem}_{rand}"
        members = [_.id for _ in random.choices(user_pool, k=random.randint(1, 40))]
        ws_group = make_group(display_name=ws_group_name, members=members, entitlements=["allow-cluster-create"])
        acc_group = make_acc_group(display_name=acct_group_name, members=members)
        return ws_group, acc_group

    return inner


@pytest.fixture
def make_ucx_group_match(make_random, make_group, make_acc_group, user_pool):
    assert (
        len(user_pool) >= 1
    ), "must have 'test-user-*' test users with id, userName and displayName in your test workspace"

    def inner():
        rand = make_random()
        ws_group_name = f"ucx_some_random_name_({rand})"
        acct_group_name = f"ucx_another_name_({rand})"
        members = [_.id for _ in random.choices(user_pool, k=random.randint(1, 40))]
        ws_group = make_group(display_name=ws_group_name, members=members, entitlements=["allow-cluster-create"])
        acc_group = make_acc_group(display_name=acct_group_name, members=members)
        return ws_group, acc_group

    return inner


class StaticTablesCrawler(TablesCrawler):
    def __init__(self, sql_backend: SqlBackend, schema: str, tables: list[TableInfo]):
        super().__init__(sql_backend, schema)
        self._tables = [
            Table(
                catalog=_.catalog_name,
                database=_.schema_name,
                name=_.name,
                object_type="TABLE" if not _.view_definition else "VIEW",
                view_text=_.view_definition,
                location=_.storage_location,
                table_format=f"{_.data_source_format}",
            )
            for _ in tables
        ]

    def snapshot(self) -> list[Table]:
        return self._tables
