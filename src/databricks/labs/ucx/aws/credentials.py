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

from databricks.labs.ucx.assessment.aws import AWSRoleAction, AWSUCRoleCandidate, AWSCredentialCandidate
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

    def create(self, name: str, role_arn: str, read_only: bool) -> StorageCredentialInfo:
        return self._ws.storage_credentials.create(
            name,
            aws_iam_role=AwsIamRoleRequest(role_arn),
            comment=f"Created by UCX during migration to UC using AWS IAM Role: {name}",
            read_only=read_only,
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
    def _print_action_plan(iam_list: list[AWSCredentialCandidate]):
        # print action plan to console for customer to review.
        for iam in iam_list:
            logger.info(f"Credential {iam.role_name} --> {iam.role_arn}: {iam.privilege}")

    def _generate_migration_list(self, include_names: set[str] | None = None) -> list[AWSCredentialCandidate]:
        """
        Create the list of IAM roles that need to be migrated, output an action plan as a csv file for users to confirm.
        It returns a list of ARNs
        """
        # load IAM role list
        iam_list = self._resource_permissions.get_roles_to_migrate()
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
            storage_credential = self._storage_credential_manager.create(
                name=iam.role_name,
                role_arn=iam.role_arn,
                read_only=iam.privilege == Privilege.READ_FILES.value,
            )
            if storage_credential.aws_iam_role is None:
                logger.error(f"Failed to create storage credential for IAM role: {iam.role_arn}")
                continue
            self._resource_permissions.update_uc_role(
                iam.role_name, iam.role_arn, storage_credential.aws_iam_role.external_id
            )
            for path in iam.paths:
                role_action = AWSRoleAction(iam.role_arn, "s3", path, iam.privilege)
                execution_result.append(self._storage_credential_manager.validate(role_action))

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

    def run(self, prompts: Prompts, *, single_role=False, role_name="UC_ROLE", policy_name="UC_POLICY"):

        iam_list = self._resource_permissions.list_uc_roles(
            single_role=single_role,
            role_name=role_name,
            policy_name=policy_name,
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
        self._resource_permissions.save_uc_compatible_roles()

    def delete_uc_roles(self, prompts: Prompts) -> None:
        uc_roles = self._resource_permissions.load_uc_compatible_roles()
        if len(uc_roles) == 0:
            self._resource_permissions.save_uc_compatible_roles()
            uc_roles = self._resource_permissions.load_uc_compatible_roles()
        storage_credentials = self._ws.storage_credentials.list()

        uc_role_mapping = {role.role_arn: role.role_name for role in uc_roles}
        selected_roles = prompts.multiple_choice_from_dict("Select the list of roles created by UCX", uc_role_mapping)
        if len(selected_roles) == 0:
            logger.info("No roles selected...")
            return
        matching_credentials = []
        for storage_credential in storage_credentials:
            if (
                storage_credential.aws_iam_role is not None
                and uc_role_mapping.get(storage_credential.aws_iam_role.role_arn) in selected_roles
            ):
                matching_credentials.append(storage_credential)

        for credential in matching_credentials:
            if credential.aws_iam_role is not None:
                logger.info(f"Storage credential: {credential.name} IAM Role: {credential.aws_iam_role.role_arn}")
        if len(matching_credentials) == 0:
            logger.info("No storage credential using the selected UC roles, proceeding to delete.")
        if len(matching_credentials) == 0 or prompts.confirm(
            "The above storage credential will be impacted on deleting the selected IAM roles,"
            " Are you sure you want to confirm"
        ):

            logger.info("Deleting UCX created roles...")
            for role_name in selected_roles:
                logger.info(f"Deleting role {role_name}.")
                self._resource_permissions.delete_uc_role(role_name)
