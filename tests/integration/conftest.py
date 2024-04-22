from collections.abc import Callable
import functools
import collections
import os
import logging
from dataclasses import replace
from functools import partial, cached_property
from datetime import timedelta

import databricks.sdk.core
import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, SchemaInfo
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.aws import AWSRoleAction
from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.azure.access import AzureResourcePermissions, StoragePermissionMapping
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.cli_command import WorkspaceContext
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import Mount, Mounts
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.install import WorkspaceInstallation, WorkspaceInstaller
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment

# pylint: disable-next=unused-wildcard-import,wildcard-import
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403
from databricks.labs.ucx.runtime import Workflows
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, GroupManager

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
                table_format=f"{_.data_source_format.value}" if _.table_type.value != "VIEW" else "",
                # type: ignore[arg-type]
            )
            for _ in tables
        ]

    def snapshot(self) -> list[Table]:
        return self._tables


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


class TestRuntimeContext(RuntimeContext):  # pylint: disable=too-many-public-methods
    def __init__(
        self, make_table_fixture, make_schema_fixture, make_udf_fixture, make_group_fixture, env_or_skip_fixture
    ):
        super().__init__()
        self._make_table = make_table_fixture
        self._make_schema = make_schema_fixture
        self._make_udf = make_udf_fixture
        self._make_group = make_group_fixture
        self._env_or_skip = env_or_skip_fixture
        self._tables: list[TableInfo] = []
        self._schemas: list[SchemaInfo] = []
        self._groups: list[Group] = []
        self._udfs = []
        self._grants = []
        # TODO: add methods to pre-populate the following:
        self._spn_infos = []

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
            self.with_aws_storage_permissions(
                [
                    AWSRoleAction(
                        self._env_or_skip("TEST_WILDCARD_INSTANCE_PROFILE"),
                        's3',
                        'WRITE_FILES',
                        f'{self._env_or_skip("TEST_MOUNT_CONTAINER")}/*',
                    )
                ]
            )

    def with_azure_storage_permissions(self, mapping: list[StoragePermissionMapping]):
        self.installation.save(mapping, filename=AzureResourcePermissions.FILENAME)

    def with_aws_storage_permissions(self, mapping: list[AWSRoleAction]):
        self.installation.save(mapping, filename=AWSResourcePermissions.INSTANCE_PROFILES_FILE_NAMES)

    def with_table_mapping_rules(self, rules):
        self.installation.save(rules, filename=TableMapping.FILENAME)

    def make_schema(self, **kwargs):
        schema_info = self._make_schema(**kwargs)
        self._schemas.append(schema_info)
        return schema_info

    def make_group(self, **kwargs):
        group_info = self._make_group(**kwargs)
        self._groups.append(group_info)
        return group_info

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
        table_info: TableInfo | None = None,
        schema_info: SchemaInfo | None = None,
        catalog: str | None = None,
        database: str | None = None,
        table: str | None = None,
        view: str | None = None,
        udf: str | None = None,
        any_file: bool = False,
        anonymous_function: bool = False,
    ):
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

    def add_table(self, table: TableInfo):
        self._tables.append(table)

    @cached_property
    def config(self) -> WorkspaceConfig:
        return WorkspaceConfig(
            warehouse_id=self._env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
            inventory_database=self.inventory_database,
            connect=self.workspace_client.config,
            renamed_group_prefix=f'tmp-{self.inventory_database}-',
            include_group_names=self.created_groups,
            include_databases=self.created_databases,
        )

    def save_tables(self, if_hiveserde: bool = False):
        # populate the tables crawled, as it is used by get_tables_to_migrate in the migrate-tables workflow
        default_table_format = "HIVE" if if_hiveserde else ""
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
            tables_to_save.append(
                Table(
                    catalog=table.catalog_name,
                    database=table.schema_name,
                    name=table.name,
                    object_type=table_type,
                    table_format=table_format,
                    location=str(table.storage_location or ""),
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

    @cached_property
    def installation(self):
        return MockInstallation()

    @cached_property
    def inventory_database(self) -> str:
        return self._make_schema(catalog_name="hive_metastore").name

    @cached_property
    def created_databases(self):
        created_databases: set[str] = set()
        for schema_info in self._schemas:
            if schema_info.catalog_name != "hive_metastore":
                continue
            created_databases.add(schema_info.name)
        for table_info in self._tables:
            if table_info.catalog_name != "hive_metastore":
                continue
            created_databases.add(table_info.schema_name)
        for grant in self._grants:
            if grant.catalog != "hive_metastore":
                continue
            if grant.database:
                created_databases.add(grant.database)
        return list(created_databases)

    @cached_property
    def created_groups(self):
        created_groups = []
        for group in self._groups:
            created_groups.append(group.display_name)
        return created_groups

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
    def group_manager(self):
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
def runtime_ctx(ws, sql_backend, make_table, make_schema, make_udf, make_group, env_or_skip):
    ctx = TestRuntimeContext(make_table, make_schema, make_udf, make_group, env_or_skip)
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


class TestInstallationContext(TestRuntimeContext):
    def __init__(
        self,
        make_table_fixture,
        make_schema_fixture,
        make_udf_fixture,
        make_group_fixture,
        env_or_skip_fixture,
        make_random_fixture,
        make_acc_group_fixture,
        make_user_fixture,
    ):
        super().__init__(
            make_table_fixture, make_schema_fixture, make_udf_fixture, make_group_fixture, env_or_skip_fixture
        )
        self._make_random = make_random_fixture
        self._make_acc_group = make_acc_group_fixture
        self._make_user = make_user_fixture

    def make_ucx_group(self, workspace_group_name=None, account_group_name=None):
        if not workspace_group_name:
            workspace_group_name = f"ucx_{self._make_random(4)}"
        if not account_group_name:
            account_group_name = workspace_group_name
        user = self._make_user()
        members = [user.id]
        ws_group = self.make_group(
            display_name=workspace_group_name,
            members=members,
            entitlements=["allow-cluster-create"],
        )
        acc_group = self._make_acc_group(display_name=account_group_name, members=members)
        return ws_group, acc_group

    @cached_property
    def running_clusters(self):
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
    def installation(self):
        return Installation(self.workspace_client, self.product_info.product_name())

    @cached_property
    def environ(self) -> dict[str, str]:
        return {**os.environ}

    @cached_property
    def workspace_installer(self):
        return WorkspaceInstaller(
            self.workspace_client,
            self.environ,
        ).replace(prompts=self.prompts, installation=self.installation, product_info=self.product_info)

    @cached_property
    def config_transform(self) -> Callable[[WorkspaceConfig], WorkspaceConfig]:
        return lambda wc: wc

    @cached_property
    def include_object_permissions(self):
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
                "table_migration": table_migration_cluster_id,
            },
            workspace_start_path=self.installation.install_folder(),
            renamed_group_prefix=self.renamed_group_prefix,
            include_group_names=self.created_groups,
            include_databases=self.created_databases,
            include_object_permissions=self.include_object_permissions,
        )
        workspace_config = self.config_transform(workspace_config)
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def product_info(self):
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def tasks(self):
        return Workflows.all().tasks()

    @cached_property
    def skip_dashboards(self):
        return True

    @cached_property
    def workflows_deployment(self):
        return WorkflowsDeployment(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.product_info.wheels(self.workspace_client),
            self.product_info,
            timedelta(minutes=3),
            self.tasks,
            skip_dashboards=self.skip_dashboards,
        )

    @cached_property
    def workspace_installation(self):
        return WorkspaceInstallation(
            self.config,
            self.installation,
            self.install_state,
            self.sql_backend,
            self.workspace_client,
            self.workflows_deployment,
            self.prompts,
            self.product_info,
            skip_dashboards=self.skip_dashboards,
        )

    @cached_property
    def extend_prompts(self):
        return {}

    @cached_property
    def renamed_group_prefix(self):
        return f"rename-{self.product_info.product_name()}-"

    @cached_property
    def prompts(self):
        return MockPrompts(
            {
                r'Open job overview in your browser.*': 'no',
                r'Do you want to uninstall ucx.*': 'yes',
                r'Do you want to delete the inventory database.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*Inventory Database.*": self.inventory_database,
                r".*Backup prefix*": self.renamed_group_prefix,
                r".*": "",
            }
            | (self.extend_prompts or {})
        )


@pytest.fixture
def installation_ctx(  # pylint: disable=too-many-arguments
    ws,
    sql_backend,
    make_table,
    make_schema,
    make_udf,
    make_group,
    env_or_skip,
    make_random,
    make_acc_group,
    make_user,
):
    ctx = TestInstallationContext(
        make_table,
        make_schema,
        make_udf,
        make_group,
        env_or_skip,
        make_random,
        make_acc_group,
        make_user,
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
    scenario = request.param
    random = make_random(5).lower()
    if_hiveserde = False
    # create external and managed tables to be migrated
    if scenario == "hiveserde_in_place":
        if_hiveserde = True
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"hiveserde_in_place_{random}")
        table_base_dir = make_storage_dir(
            path=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/hiveserde_in_place_{random}'
        )
        tables = prepare_hiveserde_tables(installation_ctx, random, schema, table_base_dir)
    else:
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"migrate_{random}")
        tables = prepare_regular_tables(installation_ctx, make_mounted_location, schema)

    # create destination catalog and schema
    dst_catalog = make_catalog()
    dst_schema = installation_ctx.make_schema(catalog_name=dst_catalog.name, name=schema.name)
    migrate_rules = [Rule.from_src_dst(table, dst_schema) for _, table in tables.items()]
    installation_ctx.with_table_mapping_rules(migrate_rules)
    installation_ctx.with_dummy_resource_permission()
    installation_ctx.save_tables(if_hiveserde=if_hiveserde)
    installation_ctx.save_mounts()
    installation_ctx.with_dummy_grants_and_tacls()
    return tables, dst_schema


@pytest.fixture
def prepared_principal_acl(runtime_ctx, env_or_skip, make_mounted_location, make_catalog, make_schema):
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
