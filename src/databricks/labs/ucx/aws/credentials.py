import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import InvalidParameterValue
from databricks.sdk.service.catalog import (
    AwsIamRoleRequest,
    Privilege,
    StorageCredentialInfo,
    ValidationResultResult,
)

from databricks.labs.ucx.assessment.aws import AWSRoleAction, AWSUCRoleCandidate
from databricks.labs.ucx.aws.access import AWSResourcePermissions

logger = logging.getLogger(__name__)


@dataclass
class CredentialValidationResult:
    name: str
    role_arn: str
    validated_on: str
    read_only: bool
    failures: list[str] | None = None


class CredentialManager:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def list(self, include_names: set[str] | None = None) -> set[str]:
        # list existed storage credentials that is using iam roles, capturing the arns
        iam_roles = set()

        storage_credentials = self._ws.storage_credentials.list(max_results=0)

        for storage_credential in storage_credentials:

            # only add storage credentials with iam roles
            if not storage_credential.aws_iam_role:
                continue

            # if include_names is not None, only add the storage credentials that are in the include_names set
            if include_names is not None and storage_credential.name not in include_names:
                continue

            iam_roles.add(storage_credential.aws_iam_role.role_arn)

        logger.info(f"Found {len(iam_roles)} distinct IAM roles already used in UC storage credentials")
        return iam_roles

    def create(self, role_action: AWSRoleAction) -> StorageCredentialInfo:
        return self._ws.storage_credentials.create(
            role_action.role_name,
            aws_iam_role=AwsIamRoleRequest(role_action.role_arn),
            comment=f"Created by UCX during migration to UC using AWS IAM Role: {role_action.role_name}",
            read_only=role_action.privilege == Privilege.READ_FILES.value,
        )

    def validate(self, role_action: AWSRoleAction) -> CredentialValidationResult:
        try:
            validation = self._ws.storage_credentials.validate(
                storage_credential_name=role_action.role_name,
                url=role_action.resource_path,
                read_only=role_action.privilege == Privilege.READ_FILES.value,
            )
        except InvalidParameterValue:
            logger.warning(
                "There is an existing external location overlaps with the prefix that is mapped to "
                "the IAM Role and used for validating the migrated storage credential. "
                "Skip the validation"
            )
            return CredentialValidationResult(
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
            return CredentialValidationResult(
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
        return CredentialValidationResult(
            role_action.role_name,
            role_action.role_arn,
            role_action.resource_path,
            role_action.privilege == Privilege.READ_FILES.value,
            None if not failures else failures,
        )


class IamRoleMigration:

    def __init__(
        self,
        installation: Installation,
        resource_permissions: AWSResourcePermissions,
        storage_credential_manager: CredentialManager,
    ):
        self._output_file = "aws_iam_role_migration_result.csv"
        self._installation = installation
        self._resource_permissions = resource_permissions
        self._storage_credential_manager = storage_credential_manager

    @staticmethod
    def _print_action_plan(iam_list: list[AWSRoleAction]):
        # print action plan to console for customer to review.
        for iam in iam_list:
            logger.info(f"{iam.role_arn}: {iam.privilege} on {iam.resource_path}")

    def _generate_migration_list(self, include_names: set[str] | None = None) -> list[AWSRoleAction]:
        """
        Create the list of IAM roles that need to be migrated, output an action plan as a csv file for users to confirm
        """
        # load IAM role list
        iam_list = self._resource_permissions.load_uc_compatible_roles()
        # list existing storage credentials
        sc_set = self._storage_credential_manager.list(include_names)
        # check if the iam is already used in UC storage credential
        filtered_iam_list = [iam for iam in iam_list if iam.role_arn not in sc_set]

        # output the action plan for customer to confirm
        self._print_action_plan(filtered_iam_list)

        return filtered_iam_list

    def save(self, migration_results: list[CredentialValidationResult]) -> str:
        return self._installation.save(migration_results, filename=self._output_file)

    def run(self, prompts: Prompts, include_names: set[str] | None = None) -> list[CredentialValidationResult]:

        iam_list = self._generate_migration_list(include_names)
        if len(iam_list) == 0:
            logger.info("No IAM Role to migrate")
            return []

        plan_confirmed = prompts.confirm(
            "Above IAM roles will be migrated to UC storage credentials, please review and confirm."
        )
        if plan_confirmed is not True:
            return []

        execution_result = []
        for iam in iam_list:
            storage_credential = self._storage_credential_manager.create(iam)
            if storage_credential.aws_iam_role is None:
                logger.error(f"Failed to create storage credential for IAM role: {iam.role_arn}")
                continue

            self._resource_permissions.update_uc_role_trust_policy(
                iam.role_name, storage_credential.aws_iam_role.external_id
            )

            execution_result.append(self._storage_credential_manager.validate(iam))

        if execution_result:
            results_file = self.save(execution_result)
            logger.info(
                f"Completed migration from IAM Role to UC Storage credentials. "
                f"Please check {results_file} for validation results"
            )
        else:
            logger.info("No IAM Role migrated to UC Storage credentials")
        return execution_result


class IamRoleCreation:

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        resource_permissions: AWSResourcePermissions,
    ):
        self._output_file = "aws_iam_role_creation_result.csv"
        self._installation = installation
        self._ws = ws
        self._resource_permissions = resource_permissions

    @staticmethod
    def _print_action_plan(iam_list: list[AWSUCRoleCandidate]):
        # print action plan to console for customer to review.
        for iam in iam_list:
            logger.info(f"Role:{iam.role_name} Policy:{iam.policy_name} Paths:{iam.resource_paths}")

    def save(self, migration_results: list[CredentialValidationResult]) -> str:
        return self._installation.save(migration_results, filename=self._output_file)

    def run(self, prompts: Prompts, *, single_role=True, role_name="UC_ROLE", policy_name="UC_POLICY"):

        iam_list = self._resource_permissions.list_uc_roles(
            single_role=single_role, role_name=role_name, policy_name=policy_name
        )
        if not iam_list:
            logger.info("No IAM Role created")
            return
        self._print_action_plan(iam_list)
        plan_confirmed = prompts.confirm(
            "Above UC Compatible IAM roles will be created and granted access to the corresponding paths, "
            "please review and confirm."
        )
        if plan_confirmed is not True:
            return

        self._resource_permissions.create_uc_roles(iam_list)
