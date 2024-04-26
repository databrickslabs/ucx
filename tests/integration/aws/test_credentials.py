import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.assessment.aws import AWSResources
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import (
    CredentialManager,
    CredentialValidationResult,
    IamRoleMigration,
)
from databricks.labs.ucx.hive_metastore import ExternalLocations


@pytest.fixture
def run_migration(ws, sql_backend, env_or_skip, runtime_ctx):
    def inner(credentials: set[str], read_only=False) -> list[CredentialValidationResult]:
        installation = MockInstallation(
            {
                "uc_roles_access.csv": [
                    {
                        'role_arn': env_or_skip("TEST_UBER_ROLE_ID"),
                        'resource_type': 's3',
                        'privilege': "READ_FILES" if read_only else "WRITE_FILES",
                        'resource_path': env_or_skip("TEST_S3_BUCKET"),
                    },
                ]
            }
        )

        aws = AWSResources(env_or_skip("AWS_DEFAULT_PROFILE"))
        location = ExternalLocations(ws, sql_backend, "inventory_schema")
        resource_permissions = AWSResourcePermissions(
            installation,
            ws,
            sql_backend,
            aws,
            location,
            runtime_ctx.principal_acl,
        )

        instance_profile_migration = IamRoleMigration(installation, ws, resource_permissions, CredentialManager(ws))

        return instance_profile_migration.run(
            MockPrompts({"Above IAM roles will be migrated to UC storage credentials *": "Yes"}),
            credentials,
        )

    return inner


def test_instance_profile_migration_existed_credential(
    env_or_skip, make_storage_credential, make_random, run_migration
):
    random = make_random(6).lower()
    credential_name = f"testinfra_cred_{random}"
    # create a storage credential for this test
    make_storage_credential(credential_name=credential_name, aws_iam_role_arn=env_or_skip("TEST_UBER_ROLE_ID"))
    # test that migration will be skipped since above storage credential already exists
    migration_result = run_migration({credential_name})

    # assert no instance profile migrated since migration_result will be empty
    assert not migration_result


@pytest.mark.parametrize("read_only", [False, True])
def test_instance_profile_migration(ws, env_or_skip, make_random, run_migration, read_only):
    credential_name = ""
    try:
        aws_migration_results = run_migration({"non-existent credential"}, read_only)
        credential_name = aws_migration_results[0].name
        storage_credential = ws.storage_credentials.get(credential_name)
    finally:
        if credential_name != "":
            ws.storage_credentials.delete(credential_name, force=True)

    assert storage_credential is not None
    assert storage_credential.read_only is read_only

    # all validation should pass
    assert not aws_migration_results[0].failures
