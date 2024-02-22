import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole, StorageCredentialInfo

from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSResourcePermissions,
    AWSResources,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend

logger = logging.getLogger(__name__)


@dataclass
class StorageCredentialValidationResult:
    name: str | None = None
    role_arn: str | None = None
    validated_on: str | None = None
    failures: str | None = None

    @classmethod
    def from_validation(cls, storage_credential: StorageCredentialInfo, failures: str | None):
        role_arn = None
        if storage_credential.aws_iam_role:
            role_arn = storage_credential.aws_iam_role.role_arn

        return cls(storage_credential.name, role_arn, failures)


class StorageCredentialManager:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def list(self) -> set[str]:
        # list existed storage credentials that is using iam roles, capturing the arns
        iam_roles = set()

        storage_credentials = self._ws.storage_credentials.list(max_results=0)

        for storage_credential in storage_credentials:

            # only add storage credentials with iam roles
            if not storage_credential.aws_iam_role:
                continue

            iam_roles.add(storage_credential.aws_iam_role.role_arn)

        logger.info(f"Found {len(iam_roles)} distinct IAM roles already used in UC storage credentials")
        return iam_roles

    def create(self, iam: AWSInstanceProfile) -> StorageCredentialValidationResult:
        if iam.iam_role_arn is None:
            logger.warning("IAM role ARN is None, skipping.")
            return StorageCredentialValidationResult.from_validation(StorageCredentialInfo(), "IAM role ARN is None.")
        storage_credential = self._ws.storage_credentials.create(
            iam.role_name,
            aws_iam_role=AwsIamRole(iam.iam_role_arn),
            comment=f"Created by UCX during migration to UC using AWS instance profile: {iam.role_name}",
        )
        return StorageCredentialValidationResult.from_validation(storage_credential, None)


class InstanceProfileMigration:

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        resource_permissions: AWSResourcePermissions,
        storage_credential_manager: StorageCredentialManager,
    ):
        self._output_file = "aws_instance_profile_migration_result.csv"
        self._installation = installation
        self._ws = ws
        self._resource_permissions = resource_permissions
        self._storage_credential_manager = storage_credential_manager

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, aws_profile: str, prompts: Prompts, product='ucx'):
        if not ws.config.is_aws:
            logger.error("Workspace is not on AWS, please run this command on a Databricks on AWS workspaces.")
            raise SystemExit()

        msg = (
            f"Have you reviewed the {AWSResourcePermissions.INSTANCE_PROFILES_FILE_NAMES} "
            "and confirm listed instance profiles to be migrated migration?"
        )
        if not prompts.confirm(msg):
            raise SystemExit()

        installation = Installation.current(ws, product)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        aws = AWSResources(aws_profile)

        resource_permissions = AWSResourcePermissions(installation, ws, sql_backend, aws, config.inventory_database)

        storage_credential_manager = StorageCredentialManager(ws)

        return cls(installation, ws, resource_permissions, storage_credential_manager)

    @staticmethod
    def _print_action_plan(iam_list: list[AWSInstanceProfile]):
        # print action plan to console for customer to review.
        for iam in iam_list:
            logger.info(f"IAM Role name: {iam.role_name}, " f"IAM Role ARN: {iam.iam_role_arn}")

    def _generate_migration_list(self) -> list[AWSInstanceProfile]:
        """
        Create the list of IAM roles that need to be migrated, output an action plan as a csv file for users to confirm
        """
        # load instance profile list from aws_instance_profile_info.csv
        iam_list = self._resource_permissions.load()
        # list existing storage credentials
        sc_set = self._storage_credential_manager.list()
        # check if the iam is already used in UC storage credential
        filtered_iam_list = [iam for iam in iam_list if iam.iam_role_arn not in sc_set]

        # output the action plan for customer to confirm
        self._print_action_plan(filtered_iam_list)

        return filtered_iam_list

    def save(self, migration_results: list[StorageCredentialValidationResult]) -> str:
        return self._installation.save(migration_results, filename=self._output_file)

    def run(self, prompts: Prompts) -> list[StorageCredentialValidationResult]:

        iam_list = self._generate_migration_list()

        plan_confirmed = prompts.confirm(
            "Above Instance Profiles will be migrated to UC storage credentials, please review and confirm."
        )
        if plan_confirmed is not True:
            return []

        execution_result = []
        for iam in iam_list:
            execution_result.append(self._storage_credential_manager.create(iam))

        if execution_result:
            results_file = self.save(execution_result)
            logger.info(
                f"Completed migration from Instance Profile to UC Storage credentials"
                f"Please check {results_file} for validation results"
            )
        else:
            logger.info("No Instance Profile migrated to UC Storage credentials")
        return execution_result
