import logging

import pytest

from databricks.labs.ucx.assessment.aws import AWSResources, AWSInstanceProfileCrawler, AWSResourcePermissions


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()



def test_role_policy(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    role_policy_actions = list(aws.get_role_policy("databricks-s3-access", "databricks-s3-access"))
    print(role_policy_actions)
    assert len(role_policy_actions) == 15


def test_instance_profile_crawler(env_or_skip, ws, sql_backend, inventory_schema):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    instance_profile_crawler = AWSInstanceProfileCrawler(ws, sql_backend, inventory_schema)
    instance_profiles = instance_profile_crawler.snapshot()
    assert instance_profiles


def test_creating_instance_profile_csv(env_or_skip, ws, sql_backend, inventory_schema, ):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    instance_profile_crawler = AWSInstanceProfileCrawler(ws, sql_backend, inventory_schema)
    aws = AWSResources(profile)
    aws_pm = AWSResourcePermissions(ws, aws, instance_profile_crawler,)
    aws_pm.save_instance_profile_permissions()
