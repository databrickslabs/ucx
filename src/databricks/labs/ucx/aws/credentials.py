import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import InvalidParameterValue
from databricks.sdk.service.catalog import (
    AwsIamRole,
    Privilege,
    StorageCredentialInfo,
    ValidationResultResult,
)

from databricks.labs.ucx.assessment.aws import (
    AWSResourcePermissions,
    AWSResources,
    AWSRoleAction,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend

logger = logging.getLogger(__name__)


@dataclass
class AWSStorageCredentialValidationResult:
    name: str
    role_arn: str
    validated_on: str
    read_only: bool
    failures: list[str] | None = None


class AWSStorageCredentialManager:
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

    def create(self, role_action: AWSRoleAction) -> StorageCredentialInfo:
        return self._ws.storage_credentials.create(
            role_action.role_name,
            aws_iam_role=AwsIamRole(role_action.role_arn),
            comment=f"Created by UCX during migration to UC using AWS instance profile: {role_action.role_name}",
        )

    def validate(self, role_action: AWSRoleAction) -> AWSStorageCredentialValidationResult:
        try:
            validation = self._ws.storage_credentials.validate(
                storage_credential_name=role_action.role_name,
                url=role_action.resource_path,
                read_only=role_action.privilege == Privilege.READ_FILES.value,
            )
        except InvalidParameterValue:
            logger.warning(
                "There is an existing external location overlaps with the prefix that is mapped to "
                "the instance profile and used for validating the migrated storage credential. "
                "Skip the validation"
            )
            return AWSStorageCredentialValidationResult(
                role_action.role_name,
                role_action.role_arn,
                role_action.resource_path,
                role_action.privilege == Privilege.READ_FILES.value,
                [
                    "The validation is skipped because an existing external location overlaps "
                    "with the location used for validation."
                ],
            )

        if not validation.results:
            return AWSStorageCredentialValidationResult(
                role_action.role_name,
                role_action.role_arn,
                role_action.resource_path,
                role_action.privilege == Privilege.READ_FILES.value,
                ["Validation returned no results."],
            )

        failures = []
        for result in validation.results:
            if result.operation is None:
                continue
            if result.result == ValidationResultResult.FAIL:
                failures.append(f"{result.operation.value} validation failed with message: {result.message}")
        return AWSStorageCredentialValidationResult(
            role_action.role_name,
            role_action.role_arn,
            role_action.resource_path,
            role_action.privilege == Privilege.READ_FILES.value,
            None if not failures else failures,
        )


class InstanceProfileMigration:

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        resource_permissions: AWSResourcePermissions,
        storage_credential_manager: AWSStorageCredentialManager,
    ):
        self._output_file = "aws_instance_profile_migration_result.csv"
        self._installation = installation
        self._ws = ws
        self._resource_permissions = resource_permissions
        self._storage_credential_manager = storage_credential_manager

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, installation: Installation, aws_profile: str, prompts: Prompts):
        if not ws.config.is_aws:
            logger.error("Workspace is not on AWS, please run this command on a Databricks on AWS workspaces.")
            raise SystemExit()

        msg = (
            f"Have you reviewed the {AWSResourcePermissions.UC_ROLES_FILE_NAMES} "
            "and confirm listed instance profiles to be migrated migration?"
        )
        if not prompts.confirm(msg):
            raise SystemExit()

        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        aws = AWSResources(aws_profile)

        resource_permissions = AWSResourcePermissions(installation, ws, sql_backend, aws, config.inventory_database)

        storage_credential_manager = AWSStorageCredentialManager(ws)

        return cls(installation, ws, resource_permissions, storage_credential_manager)

    @staticmethod
    def _print_action_plan(iam_list: list[AWSRoleAction]):
        # print action plan to console for customer to review.
        for iam in iam_list:
            logger.info(
                f"IAM Role ARN: {iam.role_arn} : " f"privilege {iam.privilege} " f"on location {iam.resource_path}"
            )

    def _generate_migration_list(self) -> list[AWSRoleAction]:
        """
        Create the list of IAM roles that need to be migrated, output an action plan as a csv file for users to confirm
        """
        # load instance profile list from aws_instance_profile_info.csv
        iam_list = self._resource_permissions.load_uc_compatible_roles()
        # list existing storage credentials
        sc_set = self._storage_credential_manager.list()
        # check if the iam is already used in UC storage credential
        filtered_iam_list = [iam for iam in iam_list if iam.role_arn not in sc_set]

        # output the action plan for customer to confirm
        self._print_action_plan(filtered_iam_list)

        return filtered_iam_list

    def save(self, migration_results: list[AWSStorageCredentialValidationResult]) -> str:
        return self._installation.save(migration_results, filename=self._output_file)

    def run(self, prompts: Prompts) -> list[AWSStorageCredentialValidationResult]:

        iam_list = self._generate_migration_list()

        plan_confirmed = prompts.confirm(
            "Above Instance Profiles will be migrated to UC storage credentials, please review and confirm."
        )
        if plan_confirmed is not True:
            return []

        execution_result = []
        for iam in iam_list:
            self._storage_credential_manager.create(iam)
            execution_result.append(self._storage_credential_manager.validate(iam))

        if execution_result:
            results_file = self.save(execution_result)
            logger.info(
                f"Completed migration from Instance Profile to UC Storage credentials"
                f"Please check {results_file} for validation results"
            )
        else:
            logger.info("No Instance Profile migrated to UC Storage credentials")
        return execution_result
