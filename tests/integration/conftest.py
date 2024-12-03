import json
from collections.abc import Callable, Generator
import functools
import collections
import os
import logging
from dataclasses import replace
from datetime import timedelta
from functools import cached_property
import shutil
import subprocess

import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import FunctionInfo, SchemaInfo, TableInfo
from databricks.sdk.service.compute import CreatePolicyResponse
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.iam import Group
from databricks.sdk.service.jobs import Job, SparkPythonTask
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, WidgetPosition, WidgetOptions, LegacyQuery

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.assessment.aws import AWSRoleAction, run_command, AWSResources
from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.azure.access import AzureResourcePermissions, StoragePermissionMapping
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import Mount, MountsCrawler, ExternalLocation, ExternalLocations
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.install import WorkspaceInstallation, WorkspaceInstaller, AccountInstaller
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation
from databricks.labs.ucx.runtime import Workflows
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, GroupManager

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture
def debug_env_name():
    return "ucws"


@pytest.fixture
def product_info():
    return "ucx", __version__


@pytest.fixture
def inventory_schema(make_schema):
    return make_schema(catalog_name="hive_metastore").name


@pytest.fixture
def make_lakeview_dashboard(ws, make_random, env_or_skip, watchdog_purge_suffix):
    """Create a lakeview dashboard."""
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")

    def create(*, display_name: str = "", query: str = "SELECT 42 AS count") -> SdkLakeviewDashboard:
        serialized_dashboard = {
            "datasets": [{"name": "query", "displayName": "count", "query": query}],
            "pages": [
                {
                    "name": "count",
                    "displayName": "Counter",
                    "layout": [
                        {
                            "widget": {
                                "name": "counter",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "query",
                                            "fields": [{"name": "count", "expression": "`count`"}],
                                            "disaggregated": True,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "counter",
                                    "encodings": {"value": {"fieldName": "count", "displayName": "count"}},
                                },
                            },
                            "position": {"x": 0, "y": 0, "width": 1, "height": 3},
                        }
                    ],
                }
            ],
        }

        if display_name:
            display_name = f"{display_name} ({make_random()})"
        else:
            display_name = f"created_by_ucx_{make_random()}_{watchdog_purge_suffix}"
        dashboard = ws.lakeview.create(
            dashboard=SdkLakeviewDashboard(
                display_name=display_name,
                serialized_dashboard=json.dumps(serialized_dashboard),
                warehouse_id=warehouse_id,
            )
        )  # type: ignore
        ws.lakeview.publish(dashboard.dashboard_id)
        return dashboard

    def delete(dashboard: SdkLakeviewDashboard) -> None:
        ws.lakeview.trash(dashboard.dashboard_id)

    yield from factory("dashboard", create, delete)


@pytest.fixture
def make_dashboard(
    ws: WorkspaceClient,
    make_random: Callable[[int], str],
    make_query,
    watchdog_purge_suffix,
):
    """Create a legacy dashboard.
    This fixture is used to test migrating legacy dashboards to Lakeview.
    """

    def create(query: LegacyQuery | None = None) -> SdkRedashDashboard:
        if not query:
            query = make_query()
        assert query
        assert query.id
        viz = ws.query_visualizations_legacy.create(
            type="table",
            query_id=query.id,
            options={
                "itemsPerPage": 1,
                "condensed": True,
                "withRowNumber": False,
                "version": 2,
                "columns": [
                    {"name": "id", "title": "id", "allowSearch": True},
                ],
            },
        )

        dashboard_name = f"ucx_D{make_random(4)}_{watchdog_purge_suffix}"
        dashboard = ws.dashboards.create(name=dashboard_name, tags=["original_dashboard_tag"])
        assert dashboard.id is not None
        ws.dashboard_widgets.create(
            dashboard_id=dashboard.id,
            visualization_id=viz.id,
            width=1,
            options=WidgetOptions(
                title="",
                position=WidgetPosition(
                    col=0,
                    row=0,
                    size_x=3,
                    size_y=3,
                ),
            ),
        )
        logger.info(f"Dashboard Created {dashboard_name}: {ws.config.host}/sql/dashboards/{dashboard.id}")
        return ws.dashboards.get(dashboard.id)  # Dashboard with widget

    def remove(dashboard: SdkRedashDashboard) -> None:
        try:
            assert dashboard.id is not None
            ws.dashboards.delete(dashboard_id=dashboard.id)
        except RuntimeError as e:
            logger.info(f"Can't delete dashboard {e}")

    yield from factory("dashboard", create, remove)


@pytest.fixture
def make_dbfs_data_copy(ws, make_cluster, env_or_skip):
    _ = make_cluster  # Need cluster to copy data
    if ws.config.is_aws:
        cmd_exec = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("TEST_WILDCARD_CLUSTER_ID"))

    def create(*, src_path: str, dst_path: str, wait_for_provisioning=True):
        @retried(on=[NotFound], timeout=timedelta(minutes=2))
        def _wait_for_provisioning(path) -> None:
            if not ws.dbfs.exists(path):
                raise NotFound(f"Location not found: {path}")

        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.cp('{src_path}', '{dst_path}', recurse=True)")
        else:
            ws.dbfs.copy(src_path, dst_path, recursive=True)
            if wait_for_provisioning:
                _wait_for_provisioning(dst_path)
        return dst_path

    def remove(dst_path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.rm('{dst_path}', recurse=True)")
        else:
            ws.dbfs.delete(dst_path, recursive=True)

    yield from factory("make_dbfs_data_copy", create, remove)


@pytest.fixture
def make_mounted_location(make_random, make_dbfs_data_copy, env_or_skip, watchdog_purge_suffix):
    """Make a copy of source data to a new location

    Use the fixture to avoid overlapping UC table path that will fail other external table migration tests.

    Note:
        This fixture is different to the other `make_` fixtures as it does not return a `Callable` to make the mounted
        location; the mounted location is made with fixture setup already.
    """
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    # DBFS locations are not purged; no suffix necessary.
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}-{watchdog_purge_suffix}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    return new_mounted_location


@pytest.fixture
def make_storage_dir(ws, env_or_skip):
    if ws.config.is_aws:
        cmd_exec = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("TEST_WILDCARD_CLUSTER_ID"))

    def create(*, path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.mkdirs('{path}')")
        else:
            ws.dbfs.mkdirs(path)
        return path

    def remove(path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.rm('{path}', recurse=True)")
        else:
            ws.dbfs.delete(path, recursive=True)

    yield from factory("make_storage_dir", create, remove)


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


@pytest.fixture
def migrated_group(acc, ws, make_group, make_acc_group):
    """Create a pair of groups in workspace and account. Assign account group to workspace."""
    ws_group = make_group()
    acc_group = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), acc_group.id, permissions=[iam.WorkspacePermission.USER])
    return MigratedGroup.partial_info(ws_group, acc_group)


@pytest.fixture
def make_ucx_group(make_random, make_group, make_acc_group, make_user, watchdog_purge_suffix):
    def inner(workspace_group_name=None, account_group_name=None, **kwargs):
        if not workspace_group_name:
            workspace_group_name = f"ucx_G{make_random(4)}-{watchdog_purge_suffix}"  # noqa: F405
        if not account_group_name:
            account_group_name = workspace_group_name
        user = make_user()
        members = [user.id]
        ws_group = make_group(
            display_name=workspace_group_name,
            members=members,
            entitlements=["allow-cluster-create"],
            **kwargs,
        )
        acc_group = make_acc_group(display_name=account_group_name, members=members, **kwargs)
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


def get_azure_spark_conf():
    return {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        "fs.azure.account.auth.type.labsazurethings.dfs.core.windows.net": "OAuth",
        "fs.azure.account.oauth.provider.type.labsazurethings.dfs.core.windows.net": "org.apache.hadoop.fs"
        ".azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id.labsazurethings.dfs.core.windows.net": "dummy_application_id",
        "fs.azure.account.oauth2.client.secret.labsazurethings.dfs.core.windows.net": "dummy",
        "fs.azure.account.oauth2.client.endpoint.labsazurethings.dfs.core.windows.net": "https://login"
        ".microsoftonline.com/directory_12345/oauth2/token",
    }


class StaticTablesCrawler(TablesCrawler):
    def __init__(self, sb: SqlBackend, schema: str, tables: list[TableInfo]):
        super().__init__(sb, schema)
        self._tables = []
        for _ in tables:
            self._tables.append(
                Table(
                    catalog=_.catalog_name,
                    database=_.schema_name,
                    name=_.name,
                    object_type=f"{_.table_type.value}",
                    view_text=_.view_definition,
                    location=_.storage_location,
                    table_format=f"{_.data_source_format.value}" if _.table_type.value != "VIEW" else "",
                    # type: ignore[arg-type]
                )
            )

    def snapshot(self, *, force_refresh: bool = False) -> list[Table]:
        return self._tables


class StaticServicePrincipalCrawler(AzureServicePrincipalCrawler):
    def __init__(self, spn_infos: list[AzureServicePrincipalInfo], *args):
        super().__init__(*args)
        self._spn_infos = spn_infos

    def snapshot(self, *, force_refresh: bool = False) -> list[AzureServicePrincipalInfo]:
        return self._spn_infos


class StaticMountCrawler(MountsCrawler):
    def __init__(
        self,
        mounts: list[Mount],
        sb: SqlBackend,
        workspace_client: WorkspaceClient,
        inventory_database: str,
    ):
        super().__init__(sb, workspace_client, inventory_database)
        self._mounts = mounts

    def snapshot(self, *, force_refresh: bool = False) -> list[Mount]:
        return self._mounts


class CommonUtils:
    def __init__(
        self,
        make_catalog_fixture,
        make_schema_fixture,
        env_or_skip_fixture,
        ws_fixture,
        make_random_fixture,
    ):
        self._make_catalog = make_catalog_fixture
        self._make_schema = make_schema_fixture
        self._env_or_skip = env_or_skip_fixture
        self._ws = ws_fixture
        self._make_random = make_random_fixture

    def with_dummy_resource_permission(self):
        # TODO: in most cases (except prepared_principal_acl) it's just a sign of a bad logic, fix it
        if self.workspace_client.config.is_azure:
            self.with_azure_storage_permissions(
                [
                    StoragePermissionMapping(
                        prefix=self._env_or_skip("TEST_MOUNT_CONTAINER"),
                        client_id='dummy_application_id',
                        principal='principal_1',
                        privilege='WRITE_FILES',
                        type='Application',
                        directory_id='directory_id_ss1',
                    )
                ]
            )
        if self.workspace_client.config.is_aws:
            instance_profile_mapping = [
                AWSRoleAction(
                    self._env_or_skip("TEST_WILDCARD_INSTANCE_PROFILE"),
                    's3',
                    'WRITE_FILES',
                    f'{self._env_or_skip("TEST_MOUNT_CONTAINER")}/*',
                )
            ]
            uc_roles_mapping = [
                AWSRoleAction(
                    self._env_or_skip("TEST_AWS_STORAGE_ROLE"),
                    's3',
                    'WRITE_FILES',
                    f'{self._env_or_skip("TEST_MOUNT_CONTAINER")}/*',
                )
            ]
            self.with_aws_storage_permissions(instance_profile_mapping, uc_roles_mapping)

    def with_azure_storage_permissions(self, mapping: list[StoragePermissionMapping]):
        self.installation.save(mapping, filename=AzureResourcePermissions.FILENAME)

    def with_aws_storage_permissions(
        self,
        instance_profile_mapping: list[AWSRoleAction],
        uc_roles_mapping: list[AWSRoleAction],
    ):
        self.installation.save(instance_profile_mapping, filename=AWSResourcePermissions.INSTANCE_PROFILES_FILE_NAME)
        self.installation.save(uc_roles_mapping, filename=AWSResourcePermissions.UC_ROLES_FILE_NAME)

    @cached_property
    def installation(self) -> Installation:
        return MockInstallation()

    @cached_property
    def inventory_database(self) -> str:
        return self._make_schema(catalog_name="hive_metastore").name

    @cached_property
    def ucx_catalog(self) -> str:
        return self._make_catalog(name=f"ucx_{self._make_random()}").name

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws


class MockRuntimeContext(CommonUtils, RuntimeContext):  # pylint: disable=too-many-instance-attributes
    def __init__(  # pylint: disable=too-many-arguments
        self,
        make_catalog_fixture,
        make_schema_fixture,
        make_table_fixture,
        make_udf_fixture,
        make_group_fixture,
        make_job_fixture,
        make_notebook_fixture,
        make_query_fixture,
        make_dashboard_fixture,
        make_lakeview_dashboard_fixture,
        make_cluster_policy_fixture,
        make_cluster_policy_permissions_fixture,
        env_or_skip_fixture,
        ws_fixture,
        make_random_fixture,
    ) -> None:
        super().__init__(
            make_catalog_fixture,
            make_schema_fixture,
            env_or_skip_fixture,
            ws_fixture,
            make_random_fixture,
        )
        RuntimeContext.__init__(self)
        self._make_table = make_table_fixture
        self._make_schema = make_schema_fixture
        self._make_udf = make_udf_fixture
        self._make_group = make_group_fixture
        self._make_job = make_job_fixture
        self._make_notebook = make_notebook_fixture
        self._make_query = make_query_fixture
        self._make_dashboard = make_dashboard_fixture
        self._make_lakeview_dashboard = make_lakeview_dashboard_fixture
        self._make_cluster_policy = make_cluster_policy_fixture
        self._make_cluster_policy_permissions = make_cluster_policy_permissions_fixture
        self._env_or_skip = env_or_skip_fixture
        self._tables: list[TableInfo] = []
        self._schemas: list[SchemaInfo] = []
        self._groups: list[Group] = []
        self._udfs: list[FunctionInfo] = []
        self._grants: list[Grant] = []
        self._jobs: list[Job] = []
        self._dashboards: list[SdkRedashDashboard | SdkLakeviewDashboard] = []
        # TODO: add methods to pre-populate the following:
        self._spn_infos: list[AzureServicePrincipalInfo] = []

    def with_table_mapping_rules(self, rules):
        self.installation.save(rules, filename=TableMapping.FILENAME)

    def with_workspace_info(self, workspace_info):
        self.installation.save(workspace_info, filename=AccountWorkspaces.SYNC_FILE_NAME)

    def make_schema(self, **kwargs) -> SchemaInfo:
        schema_info = self._make_schema(**kwargs)
        self._schemas.append(schema_info)
        return schema_info

    def make_group(self, **kwargs) -> Group:
        group_info = self._make_group(**kwargs)
        self._groups.append(group_info)
        return group_info

    def make_table(self, **kwargs) -> TableInfo:
        table_info = self._make_table(**kwargs)
        self._tables.append(table_info)
        return table_info

    def make_udf(self, **kwargs) -> FunctionInfo:
        udf_info = self._make_udf(**kwargs)
        self._udfs.append(udf_info)
        return udf_info

    def make_grant(  # pylint: disable=too-many-arguments
        self,
        principal: str,
        action_type: str,
        table_info: TableInfo | None = None,
        schema_info: SchemaInfo | None = None,
        catalog: str | None = None,
        database: str | None = None,
        table: str | None = None,
        view: str | None = None,
        udf: str | None = None,
        any_file: bool = False,
        anonymous_function: bool = False,
    ) -> Grant:
        if table_info:
            catalog = table_info.catalog_name
            database = table_info.schema_name
            table = table_info.name
        if schema_info:
            catalog = schema_info.catalog_name
            database = schema_info.name
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

    def make_cluster_policy(self, **kwargs) -> CreatePolicyResponse:
        return self._make_cluster_policy(**kwargs)

    def make_cluster_policy_permissions(self, **kwargs):
        return self._make_cluster_policy_permissions(**kwargs)

    def make_job(self, **kwargs) -> Job:
        job = self._make_job(**kwargs)
        self._jobs.append(job)
        return job

    def make_dashboard(self, **kwargs) -> SdkRedashDashboard:
        dashboard = self._make_dashboard(**kwargs)
        self._dashboards.append(dashboard)
        return dashboard

    def make_lakeview_dashboard(self, **kwargs) -> SdkLakeviewDashboard:
        dashboard = self._make_lakeview_dashboard(**kwargs)
        self._dashboards.append(dashboard)
        return dashboard

    def make_notebook(self, **kwargs):
        return self._make_notebook(**kwargs)

    def make_catalog(self, **kwargs):
        return self._make_catalog(**kwargs)

    def make_linting_resources(self) -> None:
        """Make resources to lint."""
        self.make_job(content="spark.read.parquet('dbfs://mnt/notebook/')")
        self.make_job(content="spark.table('old.stuff')")
        self.make_job(content="spark.read.parquet('dbfs://mnt/file/')", task_type=SparkPythonTask)
        self.make_job(content="spark.table('some.table')", task_type=SparkPythonTask)
        query_1 = self._make_query(sql_query='SELECT * from parquet.`dbfs://mnt/foo2/bar2`')
        self._make_dashboard(query=query_1)
        query_2 = self._make_query(sql_query='SELECT * from my_schema.my_table')
        self._make_dashboard(query=query_2)

    def add_table(self, table: TableInfo):
        self._tables.append(table)

    @cached_property
    def config(self) -> WorkspaceConfig:
        return WorkspaceConfig(
            warehouse_id=self._env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
            inventory_database=self.inventory_database,
            ucx_catalog=self.ucx_catalog,
            connect=self.workspace_client.config,
            renamed_group_prefix=f'tmp-{self.inventory_database}-',
            include_group_names=self.created_groups,
            include_databases=self.created_databases,
            include_job_ids=self.created_jobs,
            include_dashboard_ids=self.created_dashboards,
        )

    @cached_property
    def tables_crawler(self):
        """
        Returns a TablesCrawler instance with the tables that were created in the context.
        Overrides the FasterTableScanCrawler with TablesCrawler used as DBR is not available while running integration tests
        :return: TablesCrawler
        """
        return TablesCrawler(self.sql_backend, self.inventory_database, self.config.include_databases)

    def save_tables(self, is_hiveserde: bool = False):
        # populate the tables crawled, as it is used by get_tables_to_migrate in the migrate-tables workflow
        default_table_format = "HIVE" if is_hiveserde else ""
        tables_to_save = []
        for table in self._tables:
            if not table.catalog_name:
                continue
            if not table.schema_name:
                continue
            if not table.name:
                continue
            table_type = table.table_type.value if table.table_type else ""
            table_format = table.data_source_format.value if table.data_source_format else default_table_format
            storage_location = table.storage_location
            if table_type == "MANAGED":
                storage_location = ""
            tables_to_save.append(
                Table(
                    catalog=table.catalog_name,
                    database=table.schema_name,
                    name=table.name,
                    object_type=table_type,
                    table_format=table_format,
                    location=str(storage_location or ""),
                    view_text=table.view_definition,
                )
            )
        return self.sql_backend.save_table(f"{self.inventory_database}.tables", tables_to_save, Table)

    def save_mounts(self):
        return self.sql_backend.save_table(
            f"{self.inventory_database}.mounts",
            self.mounts_crawler.snapshot(),
            Mount,
        )

    def with_dummy_grants_and_tacls(self):
        # inject dummy group and table acl to avoid crawling which will slow down tests like test_table_migration_job
        self.sql_backend.save_table(
            f"{self.inventory_database}.groups",
            [
                MigratedGroup(
                    "group_id",
                    "test_group_ws",
                    "test_group_ac",
                    "tmp",
                )
            ],
            MigratedGroup,
        )
        self.sql_backend.save_table(
            f"{self.inventory_database}.grants",
            [
                Grant(
                    "test_user",
                    "SELECT",
                    database="test_database",
                    table="test_table",
                )
            ],
            Grant,
        )

    @property
    def created_databases(self) -> list[str]:
        created_databases: set[str] = set()
        for udf_info in self._udfs:
            if udf_info.catalog_name != "hive_metastore":
                continue
            assert udf_info.schema_name is not None
            created_databases.add(udf_info.schema_name)
        for schema_info in self._schemas:
            if schema_info.catalog_name != "hive_metastore":
                continue
            assert schema_info.name is not None
            created_databases.add(schema_info.name)
        for table_info in self._tables:
            if table_info.catalog_name != "hive_metastore":
                continue
            assert table_info.schema_name is not None
            created_databases.add(table_info.schema_name)
        for grant in self._grants:
            if grant.catalog != "hive_metastore":
                continue
            if grant.database:
                created_databases.add(grant.database)
        return list(created_databases)

    @property
    def created_groups(self) -> list[str]:
        return [group.display_name for group in self._groups if group.display_name is not None]

    @property
    def created_jobs(self) -> list[int]:
        return [job.job_id for job in self._jobs if job.job_id is not None]

    @property
    def created_dashboards(self) -> list[str]:
        dashboard_ids = []
        for dashboard in self._dashboards:
            if isinstance(dashboard, SdkRedashDashboard) and dashboard.id:
                dashboard_ids.append(dashboard.id)
            elif isinstance(dashboard, SdkLakeviewDashboard) and dashboard.dashboard_id:
                dashboard_ids.append(dashboard.dashboard_id)
            else:
                raise ValueError(f"Unsupported dashboard: {dashboard}")
        return dashboard_ids

    @cached_property
    def azure_service_principal_crawler(self) -> StaticServicePrincipalCrawler:
        return StaticServicePrincipalCrawler(
            self._spn_infos,
            self.workspace_client,
            self.sql_backend,
            self.inventory_database,
        )

    @cached_property
    def mounts_crawler(self) -> StaticMountCrawler:
        mount = Mount(
            f'/mnt/{self._env_or_skip("TEST_MOUNT_NAME")}/a', f'{self._env_or_skip("TEST_MOUNT_CONTAINER")}/a'
        )
        return StaticMountCrawler(
            [mount],
            self.sql_backend,
            self.workspace_client,
            self.inventory_database,
        )

    @cached_property
    def group_manager(self) -> GroupManager:
        return GroupManager(
            self.sql_backend,
            self.workspace_client,
            self.inventory_database,
            self.created_groups,
            self.config.renamed_group_prefix,
            workspace_group_regex=self.config.workspace_group_regex,
            workspace_group_replace=self.config.workspace_group_replace,
            account_group_regex=self.config.account_group_regex,
            external_id_match=self.config.group_match_by_external_id,
        )


@pytest.fixture
def runtime_ctx(  # pylint: disable=too-many-arguments
    ws,
    sql_backend,
    make_catalog,
    make_schema,
    make_table,
    make_udf,
    make_group,
    make_job,
    make_notebook,
    make_query,
    make_dashboard,
    make_lakeview_dashboard,
    make_cluster_policy,
    make_cluster_policy_permissions,
    env_or_skip,
    make_random,
) -> MockRuntimeContext:
    ctx = MockRuntimeContext(
        make_catalog,
        make_schema,
        make_table,
        make_udf,
        make_group,
        make_job,
        make_notebook,
        make_query,
        make_dashboard,
        make_lakeview_dashboard,
        make_cluster_policy,
        make_cluster_policy_permissions,
        env_or_skip,
        ws,
        make_random,
    )
    return ctx.replace(workspace_client=ws, sql_backend=sql_backend)


class MockWorkspaceContext(CommonUtils, WorkspaceContext):
    def __init__(
        self,
        make_catalog_fixture,
        make_schema_fixture,
        env_or_skip_fixture,
        ws_fixture,
        make_random_fixture,
    ):
        super().__init__(
            make_catalog_fixture,
            make_schema_fixture,
            env_or_skip_fixture,
            ws_fixture,
            make_random_fixture,
        )
        WorkspaceContext.__init__(self, ws_fixture)

    @cached_property
    def config(self) -> WorkspaceConfig:
        return WorkspaceConfig(
            warehouse_id=self._env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
            inventory_database=self.inventory_database,
            ucx_catalog=self.ucx_catalog,
            connect=self.workspace_client.config,
            renamed_group_prefix=f'tmp-{self.inventory_database}-',
        )

    def save_locations(self) -> None:
        locations: list[ExternalLocation] = []
        if self.workspace_client.config.is_azure:
            locations = [ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/a", 1)]
        if self.workspace_client.config.is_aws:
            locations = [ExternalLocation("s3://labs-things/a", 1)]
        self.sql_backend.save_table(
            f"{self.inventory_database}.external_locations",
            locations,
            ExternalLocation,
        )


class MockLocalAzureCli(MockWorkspaceContext):
    @cached_property
    def azure_cli_authenticated(self) -> bool:
        if not self.is_azure:
            pytest.skip("Azure only")
        if self.connect_config.auth_type != "azure-cli":
            pytest.skip("Local test only")
        return True

    @cached_property
    def azure_subscription_id(self) -> str:
        return self._env_or_skip("TEST_SUBSCRIPTION_ID")


@pytest.fixture
def az_cli_ctx(ws, env_or_skip, make_catalog, make_schema, make_random, sql_backend):
    ctx = MockLocalAzureCli(make_catalog, make_schema, env_or_skip, ws, make_random)
    return ctx.replace(sql_backend=sql_backend)


class MockLocalAwsCli(MockWorkspaceContext):
    @cached_property
    def aws_cli_run_command(self) -> Callable[[str | list[str]], tuple[int, str, str]]:
        if not self.is_aws:
            pytest.skip("Aws only")
        if not shutil.which("aws"):
            pytest.skip("Local test only")
        return run_command

    @cached_property
    def aws_profile(self) -> str:
        return self._env_or_skip("AWS_PROFILE")


@pytest.fixture
def aws_cli_ctx(installation_ctx, env_or_skip):
    def aws_cli_run_command():
        if not installation_ctx.is_aws:
            pytest.skip("Aws only")
        if not shutil.which("aws"):
            pytest.skip("Local test only")
        return run_command

    def aws_profile():
        return env_or_skip("AWS_PROFILE")

    @property
    def aws():
        return AWSResources(aws_profile())

    def aws_resource_permissions():
        return AWSResourcePermissions(
            installation_ctx.installation,
            installation_ctx.workspace_client,
            AWSResources(aws_profile()),
            ExternalLocations(
                installation_ctx.workspace_client,
                installation_ctx.sql_backend,
                installation_ctx.inventory_database,
                installation_ctx.tables_crawler,
                installation_ctx.mounts_crawler,
            ),
        )

    return installation_ctx.replace(
        aws_profile=aws_profile,
        aws_cli_run_command=aws_cli_run_command,
        aws_resource_permissions=aws_resource_permissions,
        connect_config=installation_ctx.workspace_client.config,
    )


class MockInstallationContext(MockRuntimeContext):
    __test__ = False

    def __init__(  # pylint: disable=too-many-arguments
        self,
        make_catalog_fixture,
        make_schema_fixture,
        make_table_fixture,
        make_udf_fixture,
        make_group_fixture,
        env_or_skip_fixture,
        make_random_fixture,
        make_acc_group_fixture,
        make_user_fixture,
        make_job_fixture,
        make_notebook_fixture,
        make_query_fixture,
        make_dashboard_fixture,
        make_cluster_policy,
        make_cluster_policy_permissions,
        ws_fixture,
        watchdog_purge_suffix,
    ):
        super().__init__(
            make_catalog_fixture,
            make_schema_fixture,
            make_table_fixture,
            make_udf_fixture,
            make_group_fixture,
            make_job_fixture,
            make_notebook_fixture,
            make_query_fixture,
            make_dashboard_fixture,
            make_cluster_policy,
            make_cluster_policy_permissions,
            env_or_skip_fixture,
            ws_fixture,
            make_random_fixture,
        )
        self._make_acc_group = make_acc_group_fixture
        self._make_user = make_user_fixture
        self._watchdog_purge_suffix = watchdog_purge_suffix

    def make_ucx_group(self, workspace_group_name=None, account_group_name=None, wait_for_provisioning=False):
        if not workspace_group_name:
            workspace_group_name = f"ucx-{self._make_random(4)}-{self._watchdog_purge_suffix}"  # noqa: F405
        if not account_group_name:
            account_group_name = workspace_group_name
        user = self._make_user()
        members = [user.id]
        ws_group = self.make_group(
            display_name=workspace_group_name,
            members=members,
            entitlements=["allow-cluster-create"],
            wait_for_provisioning=wait_for_provisioning,
        )
        acc_group = self._make_acc_group(
            display_name=account_group_name, members=members, wait_for_provisioning=wait_for_provisioning
        )
        return ws_group, acc_group

    @cached_property
    def running_clusters(self) -> tuple[str, str, str]:
        logger.debug("Waiting for clusters to start...")
        default_cluster_id = self._env_or_skip("TEST_DEFAULT_CLUSTER_ID")
        tacl_cluster_id = self._env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
        table_migration_cluster_id = self._env_or_skip("TEST_USER_ISOLATION_CLUSTER_ID")
        ensure_cluster_is_running = self.workspace_client.clusters.ensure_cluster_is_running
        Threads.strict(
            "ensure clusters running",
            [
                functools.partial(ensure_cluster_is_running, default_cluster_id),
                functools.partial(ensure_cluster_is_running, tacl_cluster_id),
                functools.partial(ensure_cluster_is_running, table_migration_cluster_id),
            ],
        )
        logger.debug("Waiting for clusters to start...")
        return default_cluster_id, tacl_cluster_id, table_migration_cluster_id

    @cached_property
    def installation(self) -> Installation:
        return Installation(self.workspace_client, self.product_info.product_name())

    @cached_property
    def account_client(self) -> AccountClient:
        return AccountClient(product="ucx", product_version=__version__)

    @cached_property
    def account_installer(self) -> AccountInstaller:
        return AccountInstaller(self.account_client)

    @cached_property
    def environ(self) -> dict[str, str]:
        return {**os.environ}

    @cached_property
    def workspace_installer(self) -> WorkspaceInstaller:
        return WorkspaceInstaller(
            self.workspace_client,
            self.environ,
        ).replace(prompts=self.prompts, installation=self.installation, product_info=self.product_info)

    @cached_property
    def config_transform(self) -> Callable[[WorkspaceConfig], WorkspaceConfig]:
        return lambda wc: wc

    @cached_property
    def include_object_permissions(self) -> None:
        return None

    @cached_property
    def config(self) -> WorkspaceConfig:
        workspace_config = self.workspace_installer.configure()
        default_cluster_id, tacl_cluster_id, table_migration_cluster_id = self.running_clusters
        workspace_config = replace(
            workspace_config,
            override_clusters={
                "main": default_cluster_id,
                "tacl": tacl_cluster_id,
                "user_isolation": table_migration_cluster_id,
            },
            workspace_start_path=self.installation.install_folder(),
            renamed_group_prefix=self.renamed_group_prefix,
            include_group_names=self.created_groups,
            include_databases=self.created_databases,
            include_job_ids=self.created_jobs,
            include_dashboard_ids=self.created_dashboards,
            include_object_permissions=self.include_object_permissions,
            warehouse_id=self._env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
            ucx_catalog=self.ucx_catalog,
        )
        workspace_config = self.config_transform(workspace_config)
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def product_info(self) -> ProductInfo:
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def tasks(self) -> list[Task]:
        return Workflows.all().tasks()

    @cached_property
    def workflows_deployment(self) -> WorkflowsDeployment:
        return WorkflowsDeployment(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.product_info.wheels(self.workspace_client),
            self.product_info,
            self.tasks,
        )

    @cached_property
    def workspace_installation(self) -> WorkspaceInstallation:
        return WorkspaceInstallation(
            self.config,
            self.installation,
            self.install_state,
            self.sql_backend,
            self.workspace_client,
            self.workflows_deployment,
            self.prompts,
            self.product_info,
        )

    @cached_property
    def progress_tracking_installation(self) -> ProgressTrackingInstallation:
        return ProgressTrackingInstallation(self.sql_backend, self.ucx_catalog)

    @cached_property
    def extend_prompts(self) -> dict[str, str]:
        return {}

    @cached_property
    def renamed_group_prefix(self) -> str:
        return f"rename-{self.product_info.product_name()}-"

    @cached_property
    def prompts(self) -> MockPrompts:
        return MockPrompts(
            {
                r'Open job overview in your browser.*': 'no',
                r'Do you want to uninstall ucx.*': 'yes',
                r'Do you want to delete the inventory database.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*Inventory Database.*": self.inventory_database,
                r".*Backup prefix*": self.renamed_group_prefix,
                r"If hive_metastore contains managed table with external.*": "1",
                r".*": "",
            }
            | (self.extend_prompts or {})
        )


@pytest.fixture
def installation_ctx(  # pylint: disable=too-many-arguments
    ws,
    sql_backend,
    make_catalog,
    make_schema,
    make_table,
    make_udf,
    make_group,
    env_or_skip,
    make_random,
    make_acc_group,
    make_user,
    make_job,
    make_notebook,
    make_query,
    make_dashboard,
    make_cluster_policy,
    make_cluster_policy_permissions,
    watchdog_purge_suffix,
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(
        make_catalog,
        make_schema,
        make_table,
        make_udf,
        make_group,
        env_or_skip,
        make_random,
        make_acc_group,
        make_user,
        make_job,
        make_notebook,
        make_query,
        make_dashboard,
        make_cluster_policy,
        make_cluster_policy_permissions,
        ws,
        watchdog_purge_suffix,
    )
    yield ctx.replace(workspace_client=ws, sql_backend=sql_backend)
    ctx.workspace_installation.uninstall()


def prepare_hiveserde_tables(context, random, schema, table_base_dir) -> dict[str, TableInfo]:
    tables: dict[str, TableInfo] = {}

    parquet_table_name = f"parquet_serde_{random}"
    parquet_ddl = f"CREATE TABLE hive_metastore.{schema.name}.{parquet_table_name} (id INT, region STRING) PARTITIONED BY (region) STORED AS PARQUETFILE LOCATION '{table_base_dir}/{parquet_table_name}'"
    tables[parquet_table_name] = context.make_table(
        schema_name=schema.name,
        name=parquet_table_name,
        hiveserde_ddl=parquet_ddl,
        storage_override=f"{table_base_dir}/{parquet_table_name}",
    )

    orc_table_name = f"orc_serde_{random}"
    orc_ddl = f"CREATE TABLE hive_metastore.{schema.name}.{orc_table_name} (id INT, region STRING) PARTITIONED BY (region) STORED AS ORC LOCATION '{table_base_dir}/{orc_table_name}'"
    tables[orc_table_name] = context.make_table(
        schema_name=schema.name,
        name=orc_table_name,
        hiveserde_ddl=orc_ddl,
        storage_override=f"{table_base_dir}/{orc_table_name}",
    )

    avro_table_name = f"avro_serde_{random}"
    avro_ddl = f"""CREATE TABLE hive_metastore.{schema.name}.{avro_table_name} (id INT, region STRING)
                        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                                  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                        TBLPROPERTIES ('avro.schema.literal'='{{
                            "namespace": "org.apache.hive",
                            "name": "first_schema",
                            "type": "record",
                            "fields": [
                                {{ "name":"id", "type":"int" }},
                                {{ "name":"region", "type":"string" }}
                            ] }}')
                        LOCATION '{table_base_dir}/{avro_table_name}'
                    """
    tables[avro_table_name] = context.make_table(
        schema_name=schema.name,
        name=avro_table_name,
        hiveserde_ddl=avro_ddl,
        storage_override=f"{table_base_dir}/{avro_table_name}",
    )
    return tables


def prepare_regular_tables(context, external_csv, schema) -> dict[str, TableInfo]:
    tables: dict[str, TableInfo] = {
        "src_managed_table": context.make_table(schema_name=schema.name),
        "src_managed_non_delta_table": context.make_table(
            catalog_name=schema.catalog_name, non_delta=True, schema_name=schema.name
        ),
        "src_external_table": context.make_table(schema_name=schema.name, external_csv=external_csv),
    }
    src_view1_text = f"SELECT * FROM {tables['src_managed_table'].full_name}"
    tables["src_view1"] = context.make_table(
        catalog_name=schema.catalog_name,
        schema_name=schema.name,
        ctas=src_view1_text,
        view=True,
    )
    src_view2_text = f"SELECT * FROM {tables['src_view1'].full_name}"
    tables["src_view2"] = context.make_table(
        catalog_name=schema.catalog_name,
        schema_name=schema.name,
        ctas=src_view2_text,
        view=True,
    )
    return tables


@pytest.fixture
def prepare_tables_for_migration(
    ws, installation_ctx, make_catalog, make_random, make_mounted_location, env_or_skip, make_storage_dir, request
) -> tuple[dict[str, TableInfo], SchemaInfo]:
    # Here we use pytest indirect parametrization, so the test function can pass arguments to this fixture and the
    # arguments will be available in the request.param. If the argument is "hiveserde", we will prepare hiveserde
    # tables, otherwise we will prepare regular tables.
    # see documents here for details https://docs.pytest.org/en/8.1.x/example/parametrize.html#indirect-parametrization
    scenario = request.param
    is_hiveserde = scenario == "hiveserde"
    random = make_random(5).lower()
    # create external and managed tables to be migrated
    if scenario == "hiveserde":
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"hiveserde_in_place_{random}")
        table_base_dir = make_storage_dir(
            path=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/hiveserde_in_place_{random}'
        )
        tables = prepare_hiveserde_tables(installation_ctx, random, schema, table_base_dir)
    elif scenario == "managed":
        schema_name = f"managed_{random}"
        schema_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/managed_{random}'
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=schema_name, location=schema_location)
        tables = prepare_regular_tables(installation_ctx, make_mounted_location, schema)
    elif scenario == "regular":
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"migrate_{random}")
        tables = prepare_regular_tables(installation_ctx, make_mounted_location, schema)

    # create destination catalog and schema
    dst_catalog = make_catalog()
    dst_schema = installation_ctx.make_schema(catalog_name=dst_catalog.name, name=schema.name)
    migrate_rules = [Rule.from_src_dst(table, dst_schema) for _, table in tables.items()]
    installation_ctx.with_table_mapping_rules(migrate_rules)
    installation_ctx.with_dummy_resource_permission()
    installation_ctx.save_tables(is_hiveserde=is_hiveserde)
    installation_ctx.save_mounts()
    installation_ctx.with_dummy_grants_and_tacls()
    return tables, dst_schema


@pytest.fixture
def prepared_principal_acl(runtime_ctx, make_mounted_location, make_catalog, make_schema):
    src_schema = make_schema(catalog_name="hive_metastore")
    src_external_table = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        external_csv=make_mounted_location,
    )
    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    rules = [Rule.from_src_dst(src_external_table, dst_schema)]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    return (
        runtime_ctx,
        f"{dst_catalog.name}.{dst_schema.name}.{src_external_table.name}",
        f"{dst_catalog.name}.{dst_schema.name}",
        dst_catalog.name,
    )


def modified_or_skip(package: str):
    info = ProductInfo.from_class(WorkspaceConfig)
    checkout_root = info.checkout_root()

    def _run(command: str) -> str:
        with subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=checkout_root,
        ) as process:
            output, error = process.communicate()
            if process.returncode != 0:
                pytest.fail(f"Command failed: {command}\n{error.decode('utf-8')}", pytrace=False)
            return output.decode("utf-8").strip()

    def check():
        if is_in_debug():
            return True  # not skipping, as we're debugging
        if 'TEST_NIGHTLY' in os.environ:
            return True  # or during nightly runs
        current_branch = _run("git branch --show-current")
        changed_files = _run(f"git diff origin/main..{current_branch} --name-only")
        if package in changed_files:
            return True
        return False

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not check():
                pytest.skip(f"Skipping long test as {package} was not modified")
            return func(*args, **kwargs)

        return wrapper

    return decorator


def pytest_ignore_collect(path):
    if not os.path.isdir(path):
        logger.debug(f"pytest_ignore_collect: not a dir: {path}")
        return False
    if is_in_debug():
        logger.debug(f"pytest_ignore_collect: in debug: {path}")
        return False  # not skipping, as we're debugging
    if 'TEST_NIGHTLY' in os.environ:
        logger.debug(f"pytest_ignore_collect: nightly: {path}")
        return False  # or during nightly runs

    checkout_root = ProductInfo.from_class(WorkspaceConfig).checkout_root()

    def _run(command: str) -> str:
        with subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=checkout_root,
        ) as process:
            output, error = process.communicate()
            if process.returncode != 0:
                raise ValueError(f"Command failed: {command}\n{error.decode('utf-8')}")
            return output.decode("utf-8").strip()

    try:  # pylint: disable=too-many-try-statements
        target_branch = os.environ.get('GITHUB_BASE_REF', 'main')
        current_branch = os.environ.get('GITHUB_HEAD_REF', _run("git branch --show-current"))
        changed_files = _run(f"git diff {target_branch}..{current_branch} --name-only")
        if path.basename in changed_files:
            logger.debug(f"pytest_ignore_collect: in changed files: {path} - {changed_files}")
            return False
        logger.debug(f"pytest_ignore_collect: skip: {path}")
        return True
    except ValueError as err:
        logger.debug(f"pytest_ignore_collect: error: {err}")
        return False
