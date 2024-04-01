import os
from datetime import timedelta
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.labs.lsql.backends import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.assessment.aws import AWSResources, run_command
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import CredentialManager, IamRoleMigration
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.credentials import (
    ServicePrincipalMigration,
    StorageCredentialManager,
)
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations, Mounts, TablesCrawler
from databricks.labs.ucx.hive_metastore.catalog_schema import CatalogSchema
from databricks.labs.ucx.hive_metastore.grants import (
    AzureACL,
    GrantsCrawler,
    PrincipalACL,
)
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.table_migrate import (
    MigrationStatusRefresher,
    TablesMigrator,
)
from databricks.labs.ucx.hive_metastore.table_move import TableMove
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from databricks.labs.ucx.installer.workflows import WorkflowsInstallation
from databricks.labs.ucx.source_code.files import Files
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager


class GlobalContext:
    flags: dict[str, str]

    @property
    def workspace_client(self) -> WorkspaceClient:
        raise ValueError("Workspace client not set")

    @property
    def sql_backend(self) -> SqlBackend:
        raise ValueError("SQL backend not set")

    @property
    def account_client(self) -> AccountClient:
        raise ValueError("Account client not set")

    @property
    def prompts(self) -> Prompts:
        raise ValueError("Prompts not set")

    @property
    def product_info(self):
        return ProductInfo.from_class(WorkspaceConfig)

    @property
    def installation(self):
        return Installation.current(self.workspace_client, self.product_info.product_name())

    @property
    def config(self):
        return self.installation.load(WorkspaceConfig)

    @property
    def permission_manager(self):
        return PermissionManager.factory(
            self.workspace_client,
            self.sql_backend,
            self.config.inventory_database,
            num_threads=self.config.num_threads,
            workspace_start_path=self.config.workspace_start_path,
        )

    @property
    def group_manager(self):
        return GroupManager(
            self.sql_backend,
            self.workspace_client,
            self.config.inventory_database,
            self.config.include_group_names,
            self.config.renamed_group_prefix,
            workspace_group_regex=self.config.workspace_group_regex,
            workspace_group_replace=self.config.workspace_group_replace,
            account_group_regex=self.config.account_group_regex,
            external_id_match=self.config.group_match_by_external_id,
        )

    @property
    def grants_crawler(self):
        return GrantsCrawler(self.tables_crawler, self.udfs_crawler, self.config.include_databases)

    @property
    def udfs_crawler(self):
        return UdfsCrawler(self.sql_backend, self.config.inventory_database, self.config.include_databases)

    @property
    def tables_crawler(self):
        return TablesCrawler(self.sql_backend, self.config.inventory_database, self.config.include_databases)

    @property
    def tables_migrator(self):
        return TablesMigrator(
            self.tables_crawler,
            self.grants_crawler,
            self.workspace_client,
            self.sql_backend,
            self.table_mapping,
            self.group_manager,
            self.migration_status_refresher,
            self.principal_acl,
        )

    @property
    def table_move(self):
        return TableMove(self.workspace_client, self.sql_backend)

    @property
    def mounts_crawler(self):
        return Mounts(self.sql_backend, self.workspace_client, self.config.inventory_database)

    @property
    def azure_service_principal_crawler(self):
        return AzureServicePrincipalCrawler(self.workspace_client, self.sql_backend, self.config.inventory_database)

    @property
    def azure_cli_authenticated(self):
        sdk_config = self.workspace_client.config
        if not sdk_config.is_azure:
            raise NotImplementedError("Azure only")
        if sdk_config.auth_type != "azure-cli":
            raise ValueError("In order to obtain AAD token, Please run azure cli to authenticate.")
        return True

    @property
    def azure_management_client(self):
        if not self.azure_cli_authenticated:
            raise NotImplementedError
        return AzureAPIClient(
            self.workspace_client.config.arm_environment.resource_manager_endpoint,
            self.workspace_client.config.arm_environment.service_management_endpoint,
        )

    @property
    def microsoft_graph_client(self):
        if not self.azure_cli_authenticated:
            raise NotImplementedError
        return AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")

    @property
    def external_locations(self):
        return ExternalLocations(self.workspace_client, self.sql_backend, self.config.inventory_database)

    @property
    def azure_resources(self):
        return AzureResources(
            self.azure_management_client,
            self.microsoft_graph_client,
            self.flags.get('include_subscriptions'),
        )

    @property
    def azure_resource_permissions(self):
        return AzureResourcePermissions(
            self.installation,
            self.workspace_client,
            self.azure_resources,
            self.external_locations,
        )

    @property
    def azure_acl(self):
        return AzureACL(
            self.workspace_client,
            self.sql_backend,
            self.azure_service_principal_crawler,
            self.azure_resource_permissions,
        )

    @property
    def principal_acl(self):
        if not self.workspace_client.config.is_azure:
            raise NotImplementedError("Azure only for now")
        eligible = self.azure_acl.get_eligible_locations_principals()
        return PrincipalACL(
            self.workspace_client,
            self.sql_backend,
            self.installation,
            self.tables_crawler,
            self.mounts_crawler,
            eligible,
        )

    @property
    def migration_status_refresher(self):
        return MigrationStatusRefresher(
            self.workspace_client,
            self.sql_backend,
            self.config.inventory_database,
            self.tables_crawler,
        )

    @property
    def aws_cli_run_command(self):
        # this is a convenience method for unit testing
        return run_command

    @property
    def aws_resources(self):
        if not self.workspace_client.config.is_aws:
            raise NotImplementedError("AWS only")
        profile = self.flags.get("aws_profile")
        profile = os.getenv("AWS_DEFAULT_PROFILE", profile)
        return AWSResources(profile, self.aws_cli_run_command)

    @property
    def aws_resource_permissions(self):
        return AWSResourcePermissions(
            self.installation,
            self.workspace_client,
            self.sql_backend,
            self.aws_resources,
            self.external_locations,
            self.config.inventory_database,
            self.flags.get("aws_account_id"),
            self.flags.get("kms_key"),
        )

    @property
    def iam_credential_manager(self):
        return CredentialManager(self.workspace_client)

    @property
    def iam_role_migration(self):
        return IamRoleMigration(
            self.installation,
            self.workspace_client,
            self.aws_resource_permissions,
            self.iam_credential_manager,
        )

    @property
    def azure_credential_manager(self):
        return StorageCredentialManager(self.workspace_client)

    @property
    def service_principal_migration(self):
        return ServicePrincipalMigration(
            self.installation,
            self.workspace_client,
            self.azure_resource_permissions,
            self.azure_service_principal_crawler,
            self.azure_credential_manager,
        )

    @property
    def azure_external_locations_migration(self):
        return ExternalLocationsMigration(
            self.workspace_client,
            self.external_locations,
            self.azure_resource_permissions,
            self.azure_resources,
        )

    @property
    def table_mapping(self):
        return TableMapping(self.installation, self.workspace_client, self.sql_backend)

    @property
    def catalog_schema(self):
        return CatalogSchema(self.workspace_client, self.table_mapping)

    @property
    def languages(self):
        index = self.tables_migrator.index()
        return Languages(index)

    @property
    def local_file_migrator(self):
        return Files(self.languages)

    @property
    def wheels(self):
        return WheelsV2(self.installation, self.product_info)

    @property
    def verify_timeout(self):
        return timedelta(minutes=2)

    @property
    def workflows(self):
        return WorkflowsInstallation(
            self.config,
            self.installation,
            self.workspace_client,
            self.wheels,
            self.prompts,
            self.product_info,
            self.verify_timeout,
        )

    @property
    def account_workspaces(self):
        return AccountWorkspaces(self.account_client)

    @property
    def workspace_info(self):
        return WorkspaceInfo(self.installation, self.workspace_client)

    @property
    def cluster_access(self):
        return ClusterAccess(self.installation, self.workspace_client, self.prompts)


class RuntimeContext(GlobalContext):
    def __init__(self, config_path: Path):
        self._config_path = config_path

    @property
    def config(self):
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @property
    def workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(config=self.config.connect, product='ucx', product_version=__version__)

    @property
    def sql_backend(self) -> SqlBackend:
        return RuntimeBackend(debug_truncate_bytes=self.config.connect.debug_truncate_bytes)

    @property
    def installation(self):
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, "ucx", install_folder=install_folder)


class CliContext(GlobalContext):
    def prompts(self) -> Prompts:
        return Prompts()


class WorkspaceContext(CliContext):
    def __init__(self, ws: WorkspaceClient, flags: dict[str, str]):
        self.flags = flags
        self._ws = ws

    @property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws

    @property
    def sql_backend(self) -> SqlBackend:
        return StatementExecutionBackend(self.workspace_client, self.config.warehouse_id)


class AccountContext(CliContext):
    def __init__(self, ac: AccountClient, flags: dict[str, str]):
        self.flags = flags
        self._ac = ac

    @property
    def account_client(self) -> AccountClient:
        if not self._ac:
            self._ac = AccountClient()
        return self._ac
