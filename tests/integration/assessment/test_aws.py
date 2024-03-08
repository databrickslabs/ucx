from databricks.labs.blueprint.installation import Installation

from databricks.labs.ucx.assessment.aws import AWSResourcePermissions, AWSResources


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()


def test_get_uc_compatible_roles(ws, sql_backend, env_or_skip, make_random, inventory_schema):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    installation = Installation(ws, make_random(4))
    aws = AWSResources(profile)
    awsrp = AWSResourcePermissions.for_cli(ws, installation, sql_backend, aws, inventory_schema)
    compat_roles = awsrp.load_uc_compatible_roles()
    print(compat_roles)
    assert compat_roles


def test_create_uc_role(env_or_skip, make_random):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    rand = make_random(5)
    role_name = f"UCX_ROLE_{rand}"
    policy_name = f"UCX_POLICY_{rand}"
    account_id = aws.validate_connection().get("Account")
    s3_prefixes = {"s3://BUCKET1/FOLDER1", "s3://BUCKET1/FOLDER1/*", "s3://BUCKET2/FOLDER2", "s3://BUCKET2/FOLDER2/*"}
    aws.add_uc_role(role_name)
    aws.add_uc_role_policy(role_name, policy_name, s3_prefixes, account_id)
    uc_roles = aws.list_all_uc_roles()
    assert role_name in [role.role_name for role in uc_roles]
