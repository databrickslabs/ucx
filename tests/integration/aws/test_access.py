import json

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.service.catalog import AwsIamRoleRequest

from databricks.labs.ucx.assessment.aws import AWSInstanceProfile, AWSResources
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.cli_command import WorkspaceContext
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


def test_create_external_location(ws, env_or_skip, make_random, inventory_schema, sql_backend, runtime_ctx):
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
    aws_permissions = AWSResourcePermissions(
        installation,
        ws,
        sql_backend,
        aws,
        ExternalLocations(ws, sql_backend, inventory_schema),
        runtime_ctx.principal_acl,
        account_id,
    )
    aws_permissions.create_external_locations(location_init=f"UCX_LOCATION_{rand}")
    external_location = [
        external_location
        for external_location in list(ws.external_locations.list())
        if external_location.name == f"ucx_location_{rand}_1"
    ]
    assert len(external_location) == 1
    assert external_location[0].url == f"s3://bucket{rand}/FOLDER1"
    assert external_location[0].credential_name == f"ucx_{rand}"


def test_create_uber_instance_profile(
    ws, env_or_skip, make_random, inventory_schema, sql_backend, make_cluster_policy, runtime_ctx
):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    account_id = aws.validate_connection().get("Account")
    sql_backend.save_table(
        f"{inventory_schema}.external_locations", [ExternalLocation("s3://bucket1/FOLDER1", 1)], ExternalLocation
    )
    installation = Installation(ws, make_random(4))
    # create a new policy
    policy = make_cluster_policy()
    installation.save(WorkspaceConfig(inventory_database='ucx', policy_id=policy.policy_id))
    # create a new uber instance profile
    aws_permissions = AWSResourcePermissions(
        installation,
        ws,
        sql_backend,
        aws,
        ExternalLocations(ws, sql_backend, inventory_schema),
        runtime_ctx.principal_acl,
        account_id,
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
