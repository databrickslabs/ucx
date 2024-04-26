import abc
import logging
import os
import shutil
from functools import cached_property

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.account import AccountWorkspaces, AccountMetastores
from databricks.labs.ucx.assessment.aws import run_command, AWSResources
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import IamRoleMigration
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.credentials import ServicePrincipalMigration, StorageCredentialManager
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.source_code.files import LocalFileMigrator
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess

logger = logging.getLogger(__name__)


class CliContext(GlobalContext, abc.ABC):
    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()


class WorkspaceContext(CliContext):
    def __init__(self, ws: WorkspaceClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return StatementExecutionBackend(self.workspace_client, self.config.warehouse_id)

    @cached_property
    def local_file_migrator(self):
        return LocalFileMigrator(self.languages)

    @cached_property
    def cluster_access(self):
        return ClusterAccess(self.installation, self.workspace_client, self.prompts)

    @cached_property
    def azure_cli_authenticated(self):
        if not self.is_azure:
            raise NotImplementedError("Azure only")
        if self.connect_config.auth_type != "azure-cli":
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
    def azure_subscription_id(self):
        subscription_id = self.named_parameters.get("subscription_id")
        if not subscription_id:
            raise ValueError("Please enter subscription id to scan storage accounts in.")
        return subscription_id

    @cached_property
    def azure_resources(self):
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
    def aws_cli_run_command(self):
        # this is a convenience method for unit testing
        if not shutil.which("aws"):
            raise ValueError("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
        return run_command

    @cached_property
    def aws_profile(self):
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
    def iam_role_migration(self):
        return IamRoleMigration(
            self.installation,
            self.aws_resource_permissions,
            self.iam_credential_manager,
        )


class AccountContext(CliContext):
    def __init__(self, ac: AccountClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ac = ac

    @cached_property
    def account_client(self) -> AccountClient:
        return self._ac

    @cached_property
    def account_workspaces(self):
        return AccountWorkspaces(self.account_client)

    @cached_property
    def account_metastores(self):
        return AccountMetastores(self.account_client)
