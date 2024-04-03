import os
from datetime import timedelta
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.account import WorkspaceInfo
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
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager

# "Service Factories" would always have a lot of pulic methods.
# This is because they are responsible for creating objects that are
# used throughout the application. That being said, we'll do best
# effort of splitting the instances between Global, Runtime,
# Workspace CLI, and Account CLI contexts.
# pylint: disable=too-many-public-methods


class GlobalContext:
    # TODO: make flags only available in CLI contexts
    flags: dict[str, str]

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        raise ValueError("Workspace client not set")

    @cached_property
    def sql_backend(self) -> SqlBackend:
        raise ValueError("SQL backend not set")

    @cached_property
    def account_client(self) -> AccountClient:
        raise ValueError("Account client not set")

    @cached_property
    def prompts(self) -> Prompts:
        raise ValueError("Prompts not set")

    @cached_property
    def product_info(self):
        return ProductInfo.from_class(WorkspaceConfig)

    @cached_property
    def installation(self):
        return Installation.current(self.workspace_client, self.product_info.product_name())

    @cached_property
    def config(self) -> WorkspaceConfig:
        return self.installation.load(WorkspaceConfig)

    @cached_property
    def permission_manager(self):
        return PermissionManager.factory(
            self.workspace_client,
            self.sql_backend,
            self.config.inventory_database,
            num_threads=self.config.num_threads,
            workspace_start_path=self.config.workspace_start_path,
        )

    @cached_property
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

    @cached_property
    def grants_crawler(self):
        return GrantsCrawler(self.tables_crawler, self.udfs_crawler, self.config.include_databases)

    @cached_property
    def udfs_crawler(self):
        return UdfsCrawler(self.sql_backend, self.config.inventory_database, self.config.include_databases)

    @cached_property
    def tables_crawler(self):
        return TablesCrawler(self.sql_backend, self.config.inventory_database, self.config.include_databases)

    @cached_property
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

    @cached_property
    def table_move(self):
        return TableMove(self.workspace_client, self.sql_backend)

    @cached_property
    def mounts_crawler(self):
        return Mounts(self.sql_backend, self.workspace_client, self.config.inventory_database)

    @cached_property
    def azure_service_principal_crawler(self):
        return AzureServicePrincipalCrawler(self.workspace_client, self.sql_backend, self.config.inventory_database)

    @cached_property
    def azure_cli_authenticated(self):
        sdk_config = self.workspace_client.config
        if not sdk_config.is_azure:
            raise NotImplementedError("Azure only")
        if sdk_config.auth_type != "azure-cli":
            raise ValueError("In order to obtain AAD token, Please run azure cli to authenticate.")
        return True

    @cached_property
    def azure_management_client(self):
        if not self.azure_cli_authenticated:
            raise NotImplementedError
        return AzureAPIClient(
            self.workspace_client.config.arm_environment.resource_manager_endpoint,
            self.workspace_client.config.arm_environment.service_management_endpoint,
        )

    @cached_property
    def microsoft_graph_client(self):
        if not self.azure_cli_authenticated:
            raise NotImplementedError
        return AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")

    @cached_property
    def external_locations(self):
        return ExternalLocations(self.workspace_client, self.sql_backend, self.config.inventory_database)

    @cached_property
    def azure_resources(self):
        return AzureResources(
            self.azure_management_client,
            self.microsoft_graph_client,
            self.flags.get('include_subscriptions'),
        )

    @cached_property
    def azure_resource_permissions(self):
        return AzureResourcePermissions(
            self.installation,
            self.workspace_client,
            self.azure_resources,
            self.external_locations,
        )

    @cached_property
    def azure_acl(self):
        return AzureACL(
            self.workspace_client,
            self.sql_backend,
            self.azure_service_principal_crawler,
            self.azure_resource_permissions,
        )

    @cached_property
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

    @cached_property
    def migration_status_refresher(self):
        return MigrationStatusRefresher(
            self.workspace_client,
            self.sql_backend,
            self.config.inventory_database,
            self.tables_crawler,
        )

    @cached_property
    def aws_cli_run_command(self):
        # this is a convenience method for unit testing
        return run_command

    @cached_property
    def aws_resources(self):
        if not self.workspace_client.config.is_aws:
            raise NotImplementedError("AWS only")
        profile = self.flags.get("aws_profile")
        profile = os.getenv("AWS_DEFAULT_PROFILE", profile)
        return AWSResources(profile, self.aws_cli_run_command)

    @cached_property
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

    @cached_property
    def iam_credential_manager(self):
        return CredentialManager(self.workspace_client)

    @cached_property
    def iam_role_migration(self):
        return IamRoleMigration(
            self.installation,
            self.workspace_client,
            self.aws_resource_permissions,
            self.iam_credential_manager,
        )

    @cached_property
    def azure_credential_manager(self):
        return StorageCredentialManager(self.workspace_client)

    @cached_property
    def service_principal_migration(self):
        return ServicePrincipalMigration(
            self.installation,
            self.workspace_client,
            self.azure_resource_permissions,
            self.azure_service_principal_crawler,
            self.azure_credential_manager,
        )

    @cached_property
    def azure_external_locations_migration(self):
        return ExternalLocationsMigration(
            self.workspace_client,
            self.external_locations,
            self.azure_resource_permissions,
            self.azure_resources,
        )

    @cached_property
    def table_mapping(self):
        return TableMapping(self.installation, self.workspace_client, self.sql_backend)

    @cached_property
    def catalog_schema(self):
        return CatalogSchema(self.workspace_client, self.table_mapping)

    @cached_property
    def languages(self):
        index = self.tables_migrator.index()
        return Languages(index)

    @cached_property
    def verify_timeout(self):
        return timedelta(minutes=2)

    @cached_property
    def wheels(self):
        return WheelsV2(self.installation, self.product_info)

    @cached_property
    def install_state(self):
        return InstallState.from_installation(self.installation)

    @cached_property
    def workflows(self):
        # TODO: decouple to only trigger jobs
        return WorkflowsInstallation(
            self.config,
            self.installation,
            self.workspace_client,
            self.wheels,
            self.prompts,
            self.product_info,
            self.verify_timeout,
        )

    @cached_property
    def workspace_info(self):
        return WorkspaceInfo(self.installation, self.workspace_client)

    @cached_property
    def cluster_access(self):
        return ClusterAccess(self.installation, self.workspace_client, self.prompts)
