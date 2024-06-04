import os
import shutil
from functools import cached_property

from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.aws import AWSResources
from databricks.labs.ucx.framework.utils import run_command
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import IamRoleMigration, IamRoleCreation
from databricks.labs.ucx.aws.locations import AWSExternalLocationsMigration
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.credentials import StorageCredentialManager, ServicePrincipalMigration
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.contexts.application import CliContext
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.files import LocalFileMigrator, LocalCodeLinter
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess


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
    def external_locations_migration(self):
        if self.is_aws:
            return AWSExternalLocationsMigration(
                self.workspace_client,
                self.external_locations,
                self.aws_resource_permissions,
                self.principal_acl,
            )
        if self.is_azure:
            return ExternalLocationsMigration(
                self.workspace_client,
                self.external_locations,
                self.azure_resource_permissions,
                self.azure_resources,
                self.principal_acl,
            )
        raise NotImplementedError

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
            self.aws_resources,
            self.external_locations,
            self.named_parameters.get("kms_key"),
        )

    @cached_property
    def iam_role_migration(self):
        return IamRoleMigration(
            self.installation,
            self.aws_resource_permissions,
            self.iam_credential_manager,
        )

    @cached_property
    def iam_role_creation(self):
        return IamRoleCreation(
            self.installation,
            self.workspace_client,
            self.aws_resource_permissions,
        )

    @cached_property
    def notebook_loader(self) -> NotebookLoader:
        return NotebookLoader()


class LocalCheckoutContext(WorkspaceContext):
    """Local context extends Workspace context to provide extra properties
    for running local operations."""

    def linter_context_factory(self):
        index = self.tables_migrator.index()
        session_state = CurrentSessionState()
        return LinterContext(index, session_state)

    @cached_property
    def local_file_migrator(self):
        return LocalFileMigrator(self.linter_context_factory)

    @cached_property
    def local_code_linter(self):
        return LocalCodeLinter(
            self.file_loader,
            self.folder_loader,
            self.path_lookup,
            self.dependency_resolver,
            self.linter_context_factory,
        )
