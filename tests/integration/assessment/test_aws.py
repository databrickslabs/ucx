from databricks.labs.ucx.assessment.aws import AWSResources


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()


def test_aws_list_uc_roles(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    print(aws.list_all_uc_roles())
    assert True
