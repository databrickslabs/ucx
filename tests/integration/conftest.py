import collections
import logging
import warnings
from functools import partial, cached_property
from typing import Callable

import databricks.sdk.core
import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.catalog import FunctionInfo, TableInfo

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)
from databricks.labs.ucx.azure.access import AzureResourcePermissions, StoragePermissionMapping
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.cli_command import WorkspaceContext
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
from databricks.labs.ucx.hive_metastore.locations import Mount, Mounts
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


class StaticGrantsCrawler(GrantsCrawler):
    def __init__(self, tc: TablesCrawler, udf: UdfsCrawler, grants: list[Grant]):
        super().__init__(tc, udf)
        self._grants = [
            Grant(
                principal=_.principal,
                action_type=_.action_type,
                catalog=_.catalog,
                database=_.database,
                table=_.table,
                view=_.view,
                udf=_.udf,
                any_file=_.any_file,
                anonymous_function=_.anonymous_function,
            )
            for _ in grants
        ]

    def snapshot(self) -> list[Grant]:
        return self._grants


class StaticTableMapping(TableMapping):
    def __init__(self, workspace_client: WorkspaceClient, sb: SqlBackend, rules: list[Rule]):
        # TODO: remove this class, it creates difficulties when used together with Permission mapping
        warnings.warn("switch to using runtime_ctx fixture", DeprecationWarning)
        installation = Installation(workspace_client, 'ucx')
        super().__init__(installation, workspace_client, sb)
        self._rules = rules

    def load(self):
        return self._rules

    def save(self, tables: TablesCrawler, workspace_info: WorkspaceInfo) -> str:
        raise RuntimeWarning("not available")


class StaticServicePrincipalCrawler(AzureServicePrincipalCrawler):
    def __init__(self, spn_infos: list[AzureServicePrincipalInfo], *args):
        super().__init__(*args)
        self._spn_infos = spn_infos

    def snapshot(self) -> list[AzureServicePrincipalInfo]:
        return self._spn_infos


class StaticMountCrawler(Mounts):
    def __init__(
        self,
        mounts: list[Mount],
        sb: SqlBackend,
        workspace_client: WorkspaceClient,
        inventory_database: str,
    ):
        super().__init__(sb, workspace_client, inventory_database)
        self._mounts = mounts

    def snapshot(self) -> list[Mount]:
        return self._mounts


class TestRuntimeContext(RuntimeContext):
    def __init__(self, make_table_fixture, make_schema_fixture, make_udf_fixture, env_or_skip_fixture):
        super().__init__()
        self._make_table = make_table_fixture
        self._make_schema = make_schema_fixture
        self._make_udf = make_udf_fixture
        self._env_or_skip = env_or_skip_fixture
        self._tables = []
        self._schemas = []
        self._udfs = []
        self._grants = []
        # TODO: add methods to pre-populate the following:
        self._spn_infos = []

    def with_dummy_azure_resource_permission(self):
        # TODO: in most cases (except prepared_principal_acl) it's just a sign of a bad logic, fix it
        self.with_azure_storage_permissions(
            [
                StoragePermissionMapping(
                    # TODO: replace with env variable
                    prefix='abfss://things@labsazurethings.dfs.core.windows.net',
                    storage_account='labsazurethings',
                    client_id='dummy_application_id',
                    principal='principal_1',
                    privilege='WRITE_FILES',
                    role_name='Storage Blob Contributor',
                    type='Application',
                    directory_id='directory_id_ss1',
                )
            ]
        )

    def with_azure_storage_permissions(self, mapping: list[StoragePermissionMapping]):
        self.installation.save(mapping, filename=AzureResourcePermissions.FILENAME)

    def with_table_mapping_rule(
        self,
        catalog_name: str,
        src_schema: str,
        dst_schema: str,
        src_table: str,
        dst_table: str,
    ):
        self.with_table_mapping_rules(
            [
                Rule(
                    workspace_name="workspace",
                    catalog_name=catalog_name,
                    src_schema=src_schema,
                    dst_schema=dst_schema,
                    src_table=src_table,
                    dst_table=dst_table,
                )
            ]
        )

    def with_table_mapping_rules(self, rules):
        self.installation.save(rules, filename=TableMapping.FILENAME)

    def make_table(self, **kwargs):
        table_info = self._make_table(**kwargs)
        self._tables.append(table_info)
        return table_info

    def make_udf(self, **kwargs):
        udf_info = self._make_udf(**kwargs)
        self._udfs.append(udf_info)
        return udf_info

    def make_grant(  # pylint: disable=too-many-arguments
        self,
        principal: str,
        action_type: str,
        catalog: str | None = None,
        database: str | None = None,
        table: str | None = None,
        view: str | None = None,
        udf: str | None = None,
        any_file: bool = False,
        anonymous_function: bool = False,
    ):
        grant = Grant(
            principal=principal,
            action_type=action_type,
            catalog=catalog,
            database=database,
            table=table,
            view=view,
            udf=udf,
            any_file=any_file,
            anonymous_function=anonymous_function,
        )
        for query in grant.hive_grant_sql():
            self.sql_backend.execute(query)
        self._grants.append(grant)
        return grant

    @cached_property
    def config(self) -> WorkspaceConfig:
        return WorkspaceConfig(
            warehouse_id=self._env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
            inventory_database=self.inventory_database,
            connect=self.workspace_client.config,
        )

    @cached_property
    def installation(self):
        # TODO: we may need to do a real installation instead of a mock
        return MockInstallation()

    @cached_property
    def inventory_database(self) -> str:
        return self._make_schema(catalog_name="hive_metastore").name

    @cached_property
    def tables_crawler(self):
        return StaticTablesCrawler(self.sql_backend, self.inventory_database, self._tables)

    @cached_property
    def udfs_crawler(self):
        return StaticUdfsCrawler(self.sql_backend, self.inventory_database, self._udfs)

    @cached_property
    def grants_crawler(self):
        return StaticGrantsCrawler(self.tables_crawler, self.udfs_crawler, self._grants)

    @cached_property
    def azure_service_principal_crawler(self):
        return StaticServicePrincipalCrawler(
            self._spn_infos,
            self.workspace_client,
            self.sql_backend,
            self.inventory_database,
        )

    @cached_property
    def mounts_crawler(self):
        # TODO: replace with env variable and make AWS and Azure versions
        real_location = 'abfss://things@labsazurethings.dfs.core.windows.net/a'
        mount = Mount(f'/mnt/{self._env_or_skip("TEST_MOUNT_NAME")}/a', real_location)
        return StaticMountCrawler(
            [mount],
            self.sql_backend,
            self.workspace_client,
            self.inventory_database,
        )


@pytest.fixture
def runtime_ctx(ws, sql_backend, make_table, make_schema, make_udf, env_or_skip):
    ctx = TestRuntimeContext(make_table, make_schema, make_udf, env_or_skip)
    return ctx.replace(workspace_client=ws, sql_backend=sql_backend)


class LocalAzureCliTest(WorkspaceContext):
    def __init__(self, _ws: WorkspaceClient, env_or_skip_fixture: Callable[[str], str]):
        super().__init__(_ws, {})
        self._env_or_skip = env_or_skip_fixture

    @cached_property
    def azure_cli_authenticated(self):
        if not self.is_azure:
            pytest.skip("Azure only")
        if self.connect_config.auth_type != "azure-cli":
            pytest.skip("Local test only")
        return True

    @cached_property
    def azure_subscription_id(self):
        return self._env_or_skip("TEST_SUBSCRIPTION_ID")


@pytest.fixture
def az_cli_ctx(ws, env_or_skip):
    return LocalAzureCliTest(ws, env_or_skip)
