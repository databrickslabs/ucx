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
def user_pool(ws):
    return list(ws.users.list(filter="displayName sw 'test-user-'", attributes="id, userName, displayName"))


@pytest.fixture
def make_ucx_group(make_random, make_group, make_acc_group, user_pool):
    assert (
        len(user_pool) >= 1
    ), "must have 'test-user-*' test users with id, userName and displayName in your test workspace"

    def inner(*, entitlements=None):
        display_name = f"ucx_{make_random(4)}"
        members = [_.id for _ in random.choices(user_pool, k=random.randint(1, 40))]
        if entitlements is None:
            entitlements = ["allow-cluster-create"]
        ws_group = make_group(display_name=display_name, members=members, entitlements=entitlements)
        acc_group = make_acc_group(display_name=display_name, members=members)
        return ws_group, acc_group

    return inner
