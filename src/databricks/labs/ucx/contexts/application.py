import abc
import logging
import os
import shutil
from datetime import timedelta
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import AccountClient, WorkspaceClient, core
from databricks.sdk.service import sql

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
from databricks.labs.ucx.hive_metastore.verification import VerifyHasMetastore
from databricks.labs.ucx.installer.workflows import DeployedWorkflows
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.workspace_access import generic, redash
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.scim import ScimSupport
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

# "Service Factories" would always have a lot of pulic methods.
# This is because they are responsible for creating objects that are
# used throughout the application. That being said, we'll do best
# effort of splitting the instances between Global, Runtime,
# Workspace CLI, and Account CLI contexts.
# pylint: disable=too-many-public-methods

logger = logging.getLogger(__name__)


class GlobalContext(abc.ABC):
    def __init__(self, named_parameters: dict[str, str] | None = None):
        if not named_parameters:
            named_parameters = {}
        self._named_parameters = named_parameters

    def replace(self, **kwargs):
        """Replace cached properties for unit testing purposes."""
        for key, value in kwargs.items():
            self.__dict__[key] = value
        return self

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
    def named_parameters(self) -> dict[str, str]:
        return self._named_parameters

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
    def connect_config(self) -> core.Config:
        return self.workspace_client.config

    @cached_property
    def is_azure(self) -> bool:
        if self.is_aws:
            return False
        return self.connect_config.is_azure

    @cached_property
    def is_aws(self) -> bool:
        return self.connect_config.is_aws

    @cached_property
    def inventory_database(self) -> str:
        return self.config.inventory_database

    @cached_property
    def workspace_listing(self):
        return generic.WorkspaceListing(
            self.workspace_client,
            self.sql_backend,
            self.inventory_database,
            self.config.num_threads,
            self.config.workspace_start_path,
        )

    @cached_property
    def generic_permissions_support(self):
        models_listing = generic.models_listing(self.workspace_client, self.config.num_threads)
        acl_listing = [
            generic.Listing(self.workspace_client.clusters.list, "cluster_id", "clusters"),
            generic.Listing(self.workspace_client.cluster_policies.list, "policy_id", "cluster-policies"),
            generic.Listing(self.workspace_client.instance_pools.list, "instance_pool_id", "instance-pools"),
            generic.Listing(self.workspace_client.warehouses.list, "id", "sql/warehouses"),
            generic.Listing(self.workspace_client.jobs.list, "job_id", "jobs"),
            generic.Listing(self.workspace_client.pipelines.list_pipelines, "pipeline_id", "pipelines"),
            generic.Listing(self.workspace_client.serving_endpoints.list, "id", "serving-endpoints"),
            generic.Listing(generic.experiments_listing(self.workspace_client), "experiment_id", "experiments"),
            generic.Listing(models_listing, "id", "registered-models"),
            generic.Listing(generic.models_root_page, "object_id", "registered-models"),
            generic.Listing(generic.tokens_and_passwords, "object_id", "authorization"),
            generic.Listing(generic.feature_store_listing(self.workspace_client), "object_id", "feature-tables"),
            generic.Listing(generic.feature_tables_root_page, "object_id", "feature-tables"),
            self.workspace_listing,
        ]
        return generic.GenericPermissionsSupport(self.workspace_client, acl_listing)

    @cached_property
    def redash_permissions_support(self):
        acl_listing = [
            redash.Listing(self.workspace_client.alerts.list, sql.ObjectTypePlural.ALERTS),
            redash.Listing(self.workspace_client.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
            redash.Listing(self.workspace_client.queries.list, sql.ObjectTypePlural.QUERIES),
        ]
        return redash.RedashPermissionsSupport(self.workspace_client, acl_listing)

    @cached_property
    def scim_entitlements_support(self):
        return ScimSupport(self.workspace_client)

    @cached_property
    def secret_scope_acl_support(self):
        return SecretScopesSupport(self.workspace_client)

    @cached_property
    def legacy_table_acl_support(self):
        return TableAclSupport(self.grants_crawler, self.sql_backend)

    @cached_property
    def permission_manager(self):
        return PermissionManager(
            self.sql_backend,
            self.inventory_database,
            [
                self.generic_permissions_support,
                self.redash_permissions_support,
                self.secret_scope_acl_support,
                self.scim_entitlements_support,
                self.legacy_table_acl_support,
            ],
        )

    @cached_property
    def group_manager(self):
        return GroupManager(
            self.sql_backend,
            self.workspace_client,
            self.inventory_database,
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
        return UdfsCrawler(self.sql_backend, self.inventory_database, self.config.include_databases)

    @cached_property
    def tables_crawler(self):
        return TablesCrawler(self.sql_backend, self.inventory_database, self.config.include_databases)

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
        return Mounts(self.sql_backend, self.workspace_client, self.inventory_database)

    @cached_property
    def azure_service_principal_crawler(self):
        return AzureServicePrincipalCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def azure_cli_authenticated(self):
        if not self.is_azure:
            raise NotImplementedError("Azure only")
        if self.connect_config.auth_type != "azure-cli":
            raise ValueError("In order to obtain AAD token, Please run azure cli to authenticate.")
        return True

    @cached_property
    def azure_subscription_id(self):
        subscription_id = self.named_parameters.get("subscription_id")
        if not subscription_id:
            raise ValueError("Please enter subscription id to scan storage accounts in.")
        return subscription_id

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
        return ExternalLocations(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def azure_resources(self):
        # TODO: move to cli_command.py
        return AzureResources(
            self.azure_management_client,
            self.microsoft_graph_client,
            [self.azure_subscription_id],
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
        if not self.is_azure:
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
            self.inventory_database,
            self.tables_crawler,
        )

    @cached_property
    def aws_cli_run_command(self):
        # TODO: slowly move this to cli_command.py
        # this is a convenience method for unit testing
        if not shutil.which("aws"):
            raise ValueError("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
        return run_command

    @cached_property
    def aws_profile(self):
        # TODO: slowly move this to cli_command.py
        aws_profile = self.named_parameters.get("aws_profile")
        if not aws_profile:
            aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
        if not aws_profile:
            raise ValueError(
                "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
                "or use the '--aws-profile=[profile-name]' parameter."
            )
        return aws_profile

    @cached_property
    def aws_resources(self):
        if not self.is_aws:
            raise NotImplementedError("AWS only")
        return AWSResources(self.aws_profile, self.aws_cli_run_command)

    @cached_property
    def aws_resource_permissions(self):
        return AWSResourcePermissions(
            self.installation,
            self.workspace_client,
            self.sql_backend,
            self.aws_resources,
            self.external_locations,
            self.inventory_database,
            self.named_parameters.get("aws_account_id"),
            self.named_parameters.get("kms_key"),
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
    def deployed_workflows(self):
        return DeployedWorkflows(self.workspace_client, self.install_state, self.verify_timeout)

    @cached_property
    def workspace_info(self):
        return WorkspaceInfo(self.installation, self.workspace_client)

    @cached_property
    def verify_has_metastore(self):
        return VerifyHasMetastore(self.workspace_client)
