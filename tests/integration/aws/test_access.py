import json

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.service.catalog import AwsIamRoleRequest
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege, PrivilegeAssignment

from databricks.labs.ucx.assessment.aws import AWSInstanceProfile, AWSResources
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.locations import AWSExternalLocationsMigration
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation


def test_get_uc_compatible_roles(ws, env_or_skip, make_random):
    installation = Installation(ws, make_random(4))
    ctx = WorkspaceContext(ws).replace(
        aws_profile=env_or_skip("AWS_DEFAULT_PROFILE"),
        installation=installation,
    )
    compat_roles = ctx.aws_resource_permissions.load_uc_compatible_roles()
    print(compat_roles)
    assert compat_roles


def test_create_external_location(ws, env_or_skip, make_random, inventory_schema, sql_backend, aws_cli_ctx):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    rand = make_random(5).lower()
    sql_backend.save_table(
        f"{inventory_schema}.external_locations", [ExternalLocation(f"s3://bucket{rand}/FOLDER1", 1)], ExternalLocation
    )
    aws = AWSResources(profile)
    role_name = f"UCX_ROLE_{rand}"
    policy_name = f"UCX_POLICY_{rand}"
    account_id = aws.validate_connection().get("Account")
    s3_prefixes = {f"bucket{rand}"}
    aws.create_uc_role(role_name)
    aws.put_role_policy(role_name, policy_name, s3_prefixes, account_id)
    ws.storage_credentials.create(
        f"ucx_{rand}",
        aws_iam_role=AwsIamRoleRequest(role_arn=f"arn:aws:iam::{account_id}:role/{role_name}"),
        read_only=False,
    )
    installation = Installation(ws, rand)
    external_location = ExternalLocations(ws, sql_backend, inventory_schema)
    aws_permissions = AWSResourcePermissions(
        installation,
        ws,
        aws,
        external_location,
        account_id,
    )
    external_location_migration = AWSExternalLocationsMigration(
        ws,
        external_location,
        aws_permissions,
        aws_cli_ctx.principal_acl,
    )
    external_location_migration.run(location_prefix=f"UCX_LOCATION_{rand}")
    external_location = [
        external_location
        for external_location in list(ws.external_locations.list())
        if external_location.name == f"ucx_location_{rand}_1"
    ]
    assert len(external_location) == 1
    assert external_location[0].url == f"s3://bucket{rand}/FOLDER1"
    assert external_location[0].credential_name == f"ucx_{rand}"


def test_create_uber_instance_profile(
    ws, env_or_skip, make_random, inventory_schema, sql_backend, make_cluster_policy, aws_cli_ctx
):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    sql_backend.save_table(
        f"{inventory_schema}.external_locations",
        [ExternalLocation("s3://bucket1/FOLDER1", 1)],
        ExternalLocation,
    )
    installation = Installation(ws, make_random(4))
    # create a new policy
    policy = make_cluster_policy()
    installation.save(WorkspaceConfig(inventory_database='ucx', policy_id=policy.policy_id))
    # create a new uber instance profile
    aws_permissions = AWSResourcePermissions(
        installation,
        ws,
        aws,
        ExternalLocations(ws, sql_backend, inventory_schema),
        aws_cli_ctx.principal_acl,
    )
    aws_permissions.create_uber_principal(
        MockPrompts(
            {
                "Do you want to create new migration role*": "yes",
                "We have identified existing UCX migration role*": "yes",
            }
        )
    )

    policy_definition = json.loads(ws.cluster_policies.get(policy_id=policy.policy_id).definition)

    config = installation.load(WorkspaceConfig)
    assert config.uber_instance_profile is not None  # check that the uber instance profile was created

    instance_profile_arn = config.uber_instance_profile
    assert policy_definition["aws_attributes.instance_profile_arn"]["value"] == instance_profile_arn
    # check that the policy definition has the correct value for the uber instance profile

    role_name = AWSInstanceProfile(instance_profile_arn).role_name
    aws.delete_instance_profile(role_name, role_name)


def test_create_external_location_validate_acl(
    make_cluster_permissions,
    ws,
    make_user,
    make_cluster,
    aws_cli_ctx,
    env_or_skip,
):
    aws_cli_ctx.with_dummy_resource_permission()
    aws_cli_ctx.save_locations()
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
    location_migration = aws_cli_ctx.external_locations_migration
    try:
        location_migration.run()
        permissions = ws.grants.get(
            SecurableType.EXTERNAL_LOCATION, env_or_skip("TEST_A_LOCATION"), principal=cluster_user.user_name
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
        ws.grants.update(
            SecurableType.EXTERNAL_LOCATION,
            env_or_skip("TEST_A_LOCATION"),
            changes=[PermissionsChange(remove=remove_aws_permissions, principal=cluster_user.user_name)],
        )
