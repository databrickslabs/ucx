import re

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.assessment.aws import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import (
    AWSStorageCredentialManager,
    AWSStorageCredentialValidationResult,
    InstanceProfileMigration,
)


@pytest.fixture
def run_migration(ws, sql_backend, env_or_skip):
    def inner(credentials: set[str], read_only=False) -> list[AWSStorageCredentialValidationResult]:
        installation = MockInstallation(
            {
                "uc_roles_access.csv": [
                    {
                        'role_arn': env_or_skip("TEST_IAM_ROLE"),
                        'resource_type': 's3',
                        'privilege': "READ_FILES" if read_only else "WRITE_FILES",
                        'resource_path': 's3://labsawsbucket/',
                    },
                ]
            }
        )

        profile = env_or_skip("AWS_DEFAULT_PROFILE")
        resource_permissions = AWSResourcePermissions.for_cli(ws, sql_backend, profile, "dont_need_a_schema")

        instance_profile_migration = InstanceProfileMigration(
            installation, ws, resource_permissions, AWSStorageCredentialManager(ws)
        )

        return instance_profile_migration.run(
            MockPrompts({"Above Instance Profiles will be migrated to UC storage credentials *": "Yes"}),
            credentials,
        )

    return inner


def test_instance_profile_migration_existed_credential(
    env_or_skip, make_storage_credential, make_random, run_migration
):
    # create a storage credential for this test
    make_storage_credential(aws_iam_role_arn=env_or_skip("TEST_IAM_ROLE"))
    random = make_random(6).lower()
    credential_name = f"testinfra_cred_{random}"
    # test that migration will be skipped since above storage credential already exists
    migration_result = run_migration({credential_name})

    # assert no instance profile migrated since migration_result will be empty
    assert not migration_result


@pytest.mark.parametrize("read_only", [False, True])
def test_instance_profile_migration(ws, env_or_skip, make_storage_credential, make_random, run_migration, read_only):

    random = make_random(6).lower()
    credential_name = f"testinfra_cred_{random}"
    env_or_skip("TEST_IAM_ROLE")

    try:
        aws_migration_results = run_migration({"non-existent credential"}, read_only)
        storage_credential = ws.storage_credentials.get(credential_name)
    finally:
        ws.storage_credentials.delete(credential_name, force=True)

    assert storage_credential is not None
    assert storage_credential.read_only is read_only

    if read_only:
        failures = aws_migration_results[0].failures
        # in this test LIST should fail as validation path does not exist
        assert failures
        match = re.match(r"LIST validation failed with message: .*The specified path does not exist", failures[0])
        assert match is not None, "LIST validation should fail"
    else:
        # all validation should pass
        assert not aws_migration_results[0].failures
