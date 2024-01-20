from databricks.labs.ucx.assessment.aws import AWSResourcePermissions, AWSResources


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()


def test_role_policy(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    role_policy_actions = aws.get_role_policy("databricks-s3-access", "databricks-s3-access")
    print(role_policy_actions)
    assert len(role_policy_actions) == 15


def test_creating_instance_profile_csv(
    env_or_skip,
    ws,
):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    aws_pm = AWSResourcePermissions(
        ws,
        aws,
    )
    aws_pm.save_instance_profile_permissions()
