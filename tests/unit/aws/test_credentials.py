import io
import logging
import re
from unittest.mock import MagicMock, create_autospec

import pytest
import yaml
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    AwsIamRole,
    AzureManagedIdentity,
    AzureServicePrincipal,
    StorageCredentialInfo,
)

from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSResourcePermissions,
)
from databricks.labs.ucx.aws.credentials import (
    InstanceProfileMigration,
    StorageCredentialManager,
)


@pytest.fixture
def ws():
    state = {
        "/Users/foo/.ucx/config.yml": yaml.dump(
            {
                'version': 2,
                'inventory_database': 'ucx',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        )
    }

    def download(path: str) -> io.StringIO:
        if path not in state:
            raise NotFound(path)
        return io.StringIO(state[path])

    ws_mock = create_autospec(WorkspaceClient)
    ws_mock.config.host = 'https://localhost'
    ws_mock.current_user.me().user_name = "foo"
    ws_mock.workspace.download = download
    return ws_mock


def side_effect_create_storage_credential(name, aws_iam_role, comment):
    return StorageCredentialInfo(name=name, aws_iam_role=aws_iam_role, comment=comment)


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

    ws.storage_credentials.create.side_effect = side_effect_create_storage_credential

    return StorageCredentialManager(ws)


def test_list_storage_credentials(credential_manager):
    assert credential_manager.list() == {
        'arn:aws:iam::123456789012:role/another-role-name',
        'arn:aws:iam::123456789012:role/example-role-name',
    }


def test_create_storage_credentials(credential_manager):
    first_iam = AWSInstanceProfile(
        iam_role_arn="arn:aws:iam::123456789012:role/example-role-name",
        instance_profile_arn="arn:aws:iam::123456789012:instance-profile/example-role-name",
    )
    second_iam = AWSInstanceProfile(
        iam_role_arn="arn:aws:iam::123456789012:role/another-role-name",
        instance_profile_arn="arn:aws:iam::123456789012:instance-profile/another-role-name",
    )

    storage_credential = credential_manager.create(first_iam)
    assert first_iam.role_name == storage_credential.name

    storage_credential = credential_manager.create(second_iam)
    assert second_iam.role_name == storage_credential.name


@pytest.fixture
def instance_profile_migration(ws, credential_manager):
    def generate_instance_profiles(num_instance_profiles: int):
        arp = create_autospec(AWSResourcePermissions)
        arp.load.return_value = [
            AWSInstanceProfile(
                iam_role_arn=f"arn:aws:iam::123456789012:role/prefix{i}",
                instance_profile_arn=f"arn:aws:iam::123456789012:instance-profile/prefix{i}",
            )
            for i in range(num_instance_profiles)
        ]

        return InstanceProfileMigration(MockInstallation(), ws, arp, credential_manager)

    return generate_instance_profiles


def test_for_cli_not_aws(caplog, ws):
    ws.config.is_aws = False
    with pytest.raises(SystemExit):
        InstanceProfileMigration.for_cli(ws, "", MagicMock())
    assert "Workspace is not on AWS, please run this command on a Databricks on AWS workspaces." in caplog.text


def test_for_cli_not_prompts(ws):
    ws.config.is_aws = True
    prompts = MockPrompts(
        {
            "Have you reviewed the aws_instance_profile_info.csv "
            "and confirm listed instance profiles to be migrated migration*": "No"
        }
    )
    with pytest.raises(SystemExit):
        InstanceProfileMigration.for_cli(ws, "", prompts)


def test_for_cli(ws):
    ws.config.is_aws = True
    prompts = MockPrompts(
        {
            "Have you reviewed the aws_instance_profile_info.csv "
            "and confirm listed instance profiles to be migrated migration*": "Yes"
        }
    )

    assert isinstance(InstanceProfileMigration.for_cli(ws, "", prompts), InstanceProfileMigration)


def test_print_action_plan(caplog, ws, instance_profile_migration):
    caplog.set_level(logging.INFO)

    prompts = MockPrompts({"Above Instance Profiles will be migrated to UC storage credentials*": "Yes"})

    instance_profile_migration(10).run(prompts)

    log_pattern = r"IAM Role name: .* IAM Role ARN: .*"
    for msg in caplog.messages:
        if re.search(log_pattern, msg):
            assert True
            return
    assert False, "Action plan is not logged"


def test_run_without_confirmation(ws, instance_profile_migration):
    prompts = MockPrompts(
        {
            "Above Instance Profiles will be migrated to UC storage credentials*": "No",
        }
    )

    assert instance_profile_migration(10).run(prompts) == []


@pytest.mark.parametrize("num_instance_profiles", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_run(ws, instance_profile_migration, num_instance_profiles: int):
    prompts = MockPrompts({"Above Instance Profiles will be migrated to UC storage credentials*": "Yes"})
    migration = instance_profile_migration(num_instance_profiles)
    results = migration.run(prompts)
    assert len(results) == num_instance_profiles
