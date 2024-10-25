from functools import cached_property
from collections.abc import Iterable
from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import WorkspaceClient, core, AccountClient
from databricks.sdk.service.catalog import SchemaInfo, TableInfo, FunctionInfo
from databricks.sdk.service.iam import Group
from databricks.sdk.service.jobs import Job, SparkPythonTask
from databricks.sdk.service.sql import Dashboard

from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.assessment.aws import AWSRoleAction
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo, AzureServicePrincipalCrawler
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.azure.access import StoragePermissionMapping, AzureResourcePermissions
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import Mount, ExternalLocation
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.workspace_access.groups import MigratedGroup
from databricks.labs.ucx import __version__
from databricks.labs.ucx.framework import crawlers


# pylint: disable=too-many-instance-attributes,too-many-public-methods,mock-no-assign,too-many-arguments


class IntegrationContext:
    def __init__(
        self,
        workspace_client,
        make_catalog_fixture,
        make_schema_fixture,
        make_table_fixture,
        make_udf_fixture,
        make_group_fixture,
        make_job_fixture,
        make_query_fixture,
        make_dashboard_fixture,
        env_or_skip_fixture,
        make_random_fixture,
    ):
        self._workspace_client = workspace_client
        self._make_catalog = make_catalog_fixture
        self._make_schema = make_schema_fixture
        self._make_table = make_table_fixture
        self._make_udf = make_udf_fixture
        self._make_group = make_group_fixture
        self._make_job = make_job_fixture
        self._make_query = make_query_fixture
        self._make_dashboard = make_dashboard_fixture
        self._env_or_skip = env_or_skip_fixture
        self._make_random = make_random_fixture
        self._schemas: list[SchemaInfo] = []
        self._tables: list[TableInfo] = []
        self._groups: list[Group] = []
        self._udfs: list[FunctionInfo] = []
        self._grants: list[Grant] = []
        self._jobs: list[Job] = []
        self._dashboards: list[Dashboard] = []
        self._spn_infos: list[AzureServicePrincipalInfo] = []

    def make_schema(self, **kwargs) -> SchemaInfo:
        schema_info = self._make_schema(**kwargs)
        self._schemas.append(schema_info)
        return schema_info

    def make_table(self, **kwargs) -> TableInfo:
        table_info = self._make_table(**kwargs)
        self.add_table(table_info)
        return table_info

    def add_table(self, table: TableInfo):
        self._tables.append(table)

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

    def save_mounts(self):
        return self.sql_backend.save_table(
            f"{self.inventory_database}.mounts",
            self.mounts_crawler.snapshot(),
            Mount,
        )

    def make_group(self, **kwargs) -> Group:
        group_info = self._make_group(**kwargs)
        self._groups.append(group_info)
        return group_info

    def make_udf(self, **kwargs) -> FunctionInfo:
        udf_info = self._make_udf(**kwargs)
        self._udfs.append(udf_info)
        return udf_info

    def with_table_mapping_rules(self, rules):
        self.installation.save(rules, filename=TableMapping.FILENAME)

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

    def with_workspace_info(self, workspace_info):
        # TODO: workspace = acc_client.workspaces.get(ws.get_workspace_id())
        self.installation.save(workspace_info, filename=AccountWorkspaces.SYNC_FILE_NAME)

    def make_linting_resources(self) -> None:
        """Make resources to lint."""
        notebook_job_1 = self._make_job(content="spark.read.parquet('dbfs://mnt/notebook/')")
        notebook_job_2 = self._make_job(content="spark.table('old.stuff')")
        file_job_1 = self._make_job(content="spark.read.parquet('dbfs://mnt/file/')", task_type=SparkPythonTask)
        file_job_2 = self._make_job(content="spark.table('some.table')", task_type=SparkPythonTask)
        query_1 = self._make_query(sql_query='SELECT * from parquet.`dbfs://mnt/foo2/bar2`')
        dashboard_1 = self._make_dashboard(query=query_1)
        query_2 = self._make_query(sql_query='SELECT * from my_schema.my_table')
        dashboard_2 = self._make_dashboard(query=query_2)

        self._jobs.extend([notebook_job_1, notebook_job_2, file_job_1, file_job_2])
        self._dashboards.append(dashboard_1)
        self._dashboards.append(dashboard_2)

    @cached_property
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

    @cached_property
    def created_groups(self) -> list[str]:
        created_groups = []
        for group in self._groups:
            if group.display_name is not None:
                created_groups.append(group.display_name)
        return created_groups

    @cached_property
    def created_jobs(self) -> list[int]:
        created_jobs = []
        for job in self._jobs:
            if job.job_id is not None:
                created_jobs.append(job.job_id)
        return created_jobs

    @cached_property
    def created_dashboards(self) -> list[str]:
        created_dashboards = []
        for dashboard in self._dashboards:
            if dashboard.id is not None:
                created_dashboards.append(dashboard.id)
        return created_dashboards

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._workspace_client

    @cached_property
    def product_info(self) -> ProductInfo:
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def account_client(self) -> AccountClient:
        host = self.connect_config.environment.deployment_url("accounts")
        return AccountClient(
            host=host,
            account_id=self._env_or_skip("DATABRICKS_ACCOUNT_ID"),
            product='ucx',
            product_version=__version__,
        )

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return StatementExecutionBackend(self.workspace_client, self.config.warehouse_id)

    @cached_property
    def connect_config(self) -> core.Config:
        return self.workspace_client.config

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

    @cached_property
    def azure_service_principal_crawler(self) -> "StaticServicePrincipalCrawler":
        return StaticServicePrincipalCrawler(self._spn_infos)

    @cached_property
    def mounts_crawler(self) -> crawlers.CrawlerBase[Mount]:
        mount = Mount(
            f'/mnt/{self._env_or_skip("TEST_MOUNT_NAME")}/a',
            f'{self._env_or_skip("TEST_MOUNT_CONTAINER")}/a',
        )
        return StaticCrawler([mount])

    @cached_property
    def installation(self) -> Installation:
        return MockInstallation()

    @cached_property
    def inventory_database(self) -> str:
        return self._make_schema(catalog_name="hive_metastore").name

    @cached_property
    def ucx_catalog(self) -> str:
        return self._make_catalog(name=f"ucx_{self._make_random()}").name


class StaticCrawler(crawlers.CrawlerBase[crawlers.Result]):
    def __init__(self, dummy: list[crawlers.Result]):
        super().__init__(
            create_autospec(SqlBackend),
            "dummy",
            "dummy",
            "dummy",
            type(dummy[0]),
        )
        self._dummy = dummy

    def _try_fetch(self) -> Iterable[crawlers.Result]:
        return self._dummy

    def _crawl(self) -> Iterable[crawlers.Result]:
        return self._dummy


class StaticServicePrincipalCrawler(AzureServicePrincipalCrawler):
    def __init__(self, dummy: list[AzureServicePrincipalInfo]):
        super().__init__(create_autospec(WorkspaceClient), create_autospec(SqlBackend), "dummy")
        self._dummy = dummy

    def _try_fetch(self) -> Iterable[AzureServicePrincipalInfo]:
        return self._dummy
