from databricks.labs.ucx.assessment.aws import AWSResources, AWSResourcePermissions


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()


def test_get_uc_compatible_roles(ws, sql_backend, env_or_skip, inventory_schema):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    awsrp = AWSResourcePermissions.for_cli(ws, sql_backend, profile, inventory_schema)
    compat_roles = awsrp.get_uc_compatible_roles()
    print(compat_roles)
    assert compat_roles


def test_create_uc_role(ws, sql_backend, env_or_skip, inventory_schema):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    awsrp = AWSResourcePermissions.for_cli(ws, sql_backend, profile, inventory_schema)
    compat_roles = awsrp.get_uc_compatible_roles()
    print(compat_roles)
    assert compat_roles
