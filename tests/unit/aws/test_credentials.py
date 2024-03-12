import logging
import re
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    AwsIamRole,
    AzureManagedIdentity,
    AzureServicePrincipal,
    Privilege,
    StorageCredentialInfo,
)

from databricks.labs.ucx.assessment.aws import AWSResources, AWSRoleAction
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import CredentialManager, IamRoleMigration
from tests.unit import DEFAULT_CONFIG
from tests.unit.azure.test_credentials import side_effect_validate_storage_credential


@pytest.fixture
def installation():
    return MockInstallation(DEFAULT_CONFIG)


@pytest.fixture
def ws():
    return create_autospec(WorkspaceClient)


def side_effect_create_aws_storage_credential(name, aws_iam_role, comment, read_only):
    return StorageCredentialInfo(name=name, aws_iam_role=aws_iam_role, comment=comment, read_only=read_only)


@pytest.fixture
def credential_manager(ws):
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(aws_iam_role=AwsIamRole(role_arn="arn:aws:iam::123456789012:role/example-role-name")),
        StorageCredentialInfo(
            azure_managed_identity=AzureManagedIdentity("/subscriptions/.../providers/Microsoft.Databricks/...")
        ),
        StorageCredentialInfo(aws_iam_role=AwsIamRole("arn:aws:iam::123456789012:role/another-role-name")),
        StorageCredentialInfo(azure_service_principal=AzureServicePrincipal("directory_id_1", "app_secret2", "secret")),
    ]

    ws.storage_credentials.create.side_effect = side_effect_create_aws_storage_credential
    ws.storage_credentials.validate.side_effect = side_effect_validate_storage_credential

    return CredentialManager(ws)


def test_list_storage_credentials(credential_manager):
    assert credential_manager.list() == {
        'arn:aws:iam::123456789012:role/another-role-name',
        'arn:aws:iam::123456789012:role/example-role-name',
    }


def test_create_storage_credentials(credential_manager):
    first_iam = AWSRoleAction(
        role_arn="arn:aws:iam::123456789012:role/example-role-name",
        resource_type="s3",
        privilege=Privilege.WRITE_FILES.value,
        resource_path="s3://example-bucket",
    )
    second_iam = AWSRoleAction(
        role_arn="arn:aws:iam::123456789012:role/another-role-name",
        resource_type="s3",
        privilege=Privilege.READ_FILES.value,
        resource_path="s3://example-bucket",
    )

    storage_credential = credential_manager.create(first_iam)
    assert first_iam.role_name == storage_credential.name

    storage_credential = credential_manager.create(second_iam)
    assert second_iam.role_name == storage_credential.name


@pytest.fixture
def instance_profile_migration(ws, installation, credential_manager):
    def generate_instance_profiles(num_instance_profiles: int):
        arp = create_autospec(AWSResourcePermissions)
        arp.load_uc_compatible_roles.return_value = [
            AWSRoleAction(
                role_arn=f"arn:aws:iam::123456789012:role/prefix{i}",
                resource_type="s3",
                privilege=Privilege.WRITE_FILES.value,
                resource_path=f"s3://example-bucket-{i}",
            )
            for i in range(num_instance_profiles)
        ]

        return IamRoleMigration(installation, ws, arp, credential_manager)

    return generate_instance_profiles


def test_for_cli_not_aws(caplog, ws, installation):
    ws.config.is_aws = False
    with pytest.raises(SystemExit):
        IamRoleMigration.for_cli(ws, installation, MagicMock(), MockPrompts({}))
    assert "Workspace is not on AWS, please run this command on a Databricks on AWS workspaces." in caplog.text


def test_for_cli_not_prompts(ws, installation):
    ws.config.is_aws = True
    prompts = MockPrompts(
        {
            f"Have you reviewed the {AWSResourcePermissions.UC_ROLES_FILE_NAMES} "
            "and confirm listed IAM roles to be migrated*": "No"
        }
    )
    with pytest.raises(SystemExit):
        IamRoleMigration.for_cli(ws, installation, MagicMock(), prompts)


def test_for_cli(ws, installation):
    ws.config.is_aws = True
    prompts = MockPrompts(
        {
            f"Have you reviewed the {AWSResourcePermissions.UC_ROLES_FILE_NAMES} "
            "and confirm listed IAM roles to be migrated*": "Yes"
        }
    )
    aws = create_autospec(AWSResources)
    aws.validate_connection.return_value = {"Account": "123456789012"}

    assert isinstance(IamRoleMigration.for_cli(ws, installation, aws, prompts), IamRoleMigration)


def test_print_action_plan(caplog, ws, instance_profile_migration):
    caplog.set_level(logging.INFO)

    prompts = MockPrompts({"Above IAM roles will be migrated to UC storage credentials*": "Yes"})

    instance_profile_migration(10).run(prompts)

    log_pattern = r"arn:aws:iam:.* on s3:.*"
    for msg in caplog.messages:
        if re.search(log_pattern, msg):
            assert True
            return
    assert False, "Action plan is not logged"


def test_run_without_confirmation(ws, instance_profile_migration):
    prompts = MockPrompts(
        {
            "Above IAM roles will be migrated to UC storage credentials*": "No",
        }
    )

    assert instance_profile_migration(10).run(prompts) == []


@pytest.mark.parametrize("num_instance_profiles", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_run(ws, instance_profile_migration, num_instance_profiles: int):
    prompts = MockPrompts({"Above IAM roles will be migrated to UC storage credentials*": "Yes"})
    migration = instance_profile_migration(num_instance_profiles)
    results = migration.run(prompts)
    assert len(results) == num_instance_profiles


def test_validate_read_only_storage_credentials(credential_manager):
    role_action = AWSRoleAction("arn:aws:iam::123456789012:role/client_id", "s3", "READ_FILES", "s3://prefix")

    # validate read-only storage credential
    validation = credential_manager.validate(role_action)
    assert validation.read_only is True
    assert validation.name == role_action.role_name
    assert not validation.failures


def test_validate_storage_credentials_overlap_location(credential_manager):
    role_action = AWSRoleAction("arn:aws:iam::123456789012:role/overlap", "s3", "READ_FILES", "s3://prefix")

    # prefix used for validation overlaps with existing external location will raise InvalidParameterValue
    # assert InvalidParameterValue is handled
    validation = credential_manager.validate(role_action)
    assert validation.failures == [
        "The validation is skipped because "
        "an existing external location overlaps with the location used for validation."
    ]


def test_validate_storage_credentials_non_response(credential_manager):
    permission_mapping = AWSRoleAction("arn:aws:iam::123456789012:role/none", "s3", "READ_FILES", "s3://prefix")

    validation = credential_manager.validate(permission_mapping)
    assert validation.failures == ["Validation returned no results."]


def test_validate_storage_credentials_failed_operation(credential_manager):
    permission_mapping = AWSRoleAction("arn:aws:iam::123456789012:role/fail", "s3", "READ_FILES", "s3://prefix")

    validation = credential_manager.validate(permission_mapping)
    assert validation.failures == ["LIST validation failed with message: fail"]
