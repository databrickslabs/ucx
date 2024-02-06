from databricks.labs.ucx.assessment.aws import AWSResources, AWSResourcePermissions


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()


def test_get_uc_compatible_roles(ws, env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    awsrp = AWSResourcePermissions.for_cli(ws, profile)
    compat_roles = awsrp.get_uc_compatible_roles()
    print(compat_roles)
    assert compat_roles
