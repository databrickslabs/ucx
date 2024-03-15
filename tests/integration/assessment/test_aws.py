from databricks.labs.ucx.assessment.aws import AWSResources


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()


def test_create_uc_role(env_or_skip, make_random):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    rand = make_random(5)
    role_name = f"UCX_ROLE_{rand}"
    policy_name = f"UCX_POLICY_{rand}"
    account_id = aws.validate_connection().get("Account")
    s3_prefixes = {"s3://BUCKET1/FOLDER1", "s3://BUCKET1/FOLDER1/*", "s3://BUCKET2/FOLDER2", "s3://BUCKET2/FOLDER2/*"}
    aws.create_uc_role(role_name)
    aws.put_role_policy(role_name, policy_name, s3_prefixes, account_id)
    uc_roles = aws.list_all_uc_roles()
    assert role_name in [role.role_name for role in uc_roles]
