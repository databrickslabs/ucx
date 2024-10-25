import dataclasses
import json
from pathlib import Path

from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.service.catalog import AwsIamRoleRequest
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege, PrivilegeAssignment

from databricks.labs.ucx.assessment.aws import AWSInstanceProfile, AWSResources, AWSUCRoleCandidate
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.locations import AWSExternalLocationsMigration
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation


def test_create_external_location(ws, env_or_skip, make_random, inventory_schema, sql_backend, aws_cli_ctx):
    profile = env_or_skip("AWS_PROFILE")
    aws_cli_ctx.workspace_installation.run()
    rand = make_random(5).lower()
    sql_backend.save_table(
        f"{inventory_schema}.external_locations",
        [ExternalLocation(f"s3://bucket{rand}/FOLDER1", 1), ExternalLocation(f"s3://bucket{rand}/FOLDER2", 1)],
        ExternalLocation,
    )
    aws = AWSResources(profile)
    role_name = f"UCX_ROLE_{rand}"
    policy_name = f"UCX_POLICY_{rand}"
    account_id = aws.validate_connection().get("Account")
    s3_prefixes = {f"s3://bucket{rand}"}

    aws_permissions = AWSResourcePermissions(
        aws_cli_ctx.installation,
        ws,
        aws,
        aws_cli_ctx.external_locations,
        account_id,
    )
    role_candidate = AWSUCRoleCandidate(role_name, policy_name, list(s3_prefixes))
    aws_permissions.create_uc_roles([role_candidate])

    credential = ws.storage_credentials.create(
        f"ucx_{rand}",
        aws_iam_role=AwsIamRoleRequest(role_arn=f"arn:aws:iam::{account_id}:role/{role_name}"),
        read_only=False,
    )
    aws.update_uc_role(role_name, f"arn:aws:iam::{account_id}:role/{role_name}", credential.id)

    external_location_migration = AWSExternalLocationsMigration(
        ws,
        aws_cli_ctx.external_locations,
        aws_permissions,
        aws_cli_ctx.principal_acl,
    )
    external_location_migration.run()
    out = []
    for external_location in list(ws.external_locations.list()):
        if external_location.name == f"bucket{rand}_folder1":
            out.append(external_location)
    assert len(out) == 1
    assert out[0].url == f"s3://bucket{rand}/FOLDER1"
    assert out[0].credential_name == f"ucx_{rand}"


def test_create_uber_instance_profile(ws, env_or_skip, make_random, make_cluster_policy, aws_cli_ctx):
    env_or_skip("AWS_PROFILE")
    aws_cli_ctx.workspace_installation.run()
    aws_cli_ctx.sql_backend.save_table(
        f"{aws_cli_ctx.inventory_database}.external_locations",
        [ExternalLocation("s3://bucket1/FOLDER1", 1)],
        ExternalLocation,
    )
    # create a new policy
    policy = make_cluster_policy()
    aws_cli_ctx.installation.save(
        dataclasses.replace(aws_cli_ctx.workspace_installation.config, policy_id=policy.policy_id)
    )
    # create a new uber instance profile
    aws_cli_ctx.save_tables()
    aws_cli_ctx.aws_resource_permissions.create_uber_principal(
        MockPrompts(
            {
                "Do you want to create new migration role*": "yes",
                "We have identified existing UCX migration role*": "yes",
                ".*": "no",
            }
        )
    )

    policy_definition = json.loads(ws.cluster_policies.get(policy_id=policy.policy_id).definition)

    config = aws_cli_ctx.installation.load(WorkspaceConfig)
    assert config.uber_instance_profile is not None  # check that the uber instance profile was created

    instance_profile_arn = config.uber_instance_profile
    assert policy_definition["aws_attributes.instance_profile_arn"]["value"] == instance_profile_arn
    # check that the policy definition has the correct value for the uber instance profile

    role_name = AWSInstanceProfile(instance_profile_arn).role_name
    AWSResources(aws_cli_ctx.aws_profile()).delete_instance_profile(role_name, role_name)


def test_create_external_location_validate_acl(
    make_cluster_permissions,
    ws,
    make_user,
    make_cluster,
    aws_cli_ctx,
    env_or_skip,
    inventory_schema,
):
    aws_cli_ctx.workspace_installation.run()
    aws_cli_ctx.with_dummy_resource_permission()
    aws_cli_ctx.sql_backend.save_table(
        f"{inventory_schema}.external_locations",
        [
            ExternalLocation(f"{env_or_skip('TEST_MOUNT_CONTAINER')}", 1),
        ],
        ExternalLocation,
    )
    external_location_name = "_".join(Path(env_or_skip("TEST_MOUNT_CONTAINER").lower()).parts[1:])
    cluster = make_cluster(
        single_node=True,
        data_security_mode=DataSecurityMode.NONE,
        aws_attributes=AwsAttributes(instance_profile_arn=env_or_skip("TEST_WILDCARD_INSTANCE_PROFILE")),
    )
    cluster_user = make_user()
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_RESTART,
        user_name=cluster_user.user_name,
    )
    account_id = aws_cli_ctx.aws.validate_connection().get("Account")
    aws_permissions = AWSResourcePermissions(
        aws_cli_ctx.installation,
        ws,
        aws_cli_ctx.aws,
        aws_cli_ctx.external_locations,
        account_id,
    )
    external_location_migration = AWSExternalLocationsMigration(
        ws,
        aws_cli_ctx.external_locations,
        aws_permissions,
        aws_cli_ctx.principal_acl,
    )
    try:
        external_location_migration.run()
        permissions = ws.grants.get(
            SecurableType.EXTERNAL_LOCATION, external_location_name, principal=cluster_user.user_name
        )
        expected_aws_permission = PrivilegeAssignment(
            principal=cluster_user.user_name,
            privileges=[Privilege.CREATE_EXTERNAL_TABLE, Privilege.CREATE_EXTERNAL_VOLUME, Privilege.READ_FILES],
        )
        assert expected_aws_permission in permissions.privilege_assignments
    finally:
        remove_aws_permissions = [
            Privilege.CREATE_EXTERNAL_TABLE,
            Privilege.CREATE_EXTERNAL_VOLUME,
            Privilege.READ_FILES,
        ]
        ws.external_locations.delete(external_location_name)
        ws.grants.update(
            SecurableType.EXTERNAL_LOCATION,
            env_or_skip("TEST_A_LOCATION"),
            changes=[PermissionsChange(remove=remove_aws_permissions, principal=cluster_user.user_name)],
        )
