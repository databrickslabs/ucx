import logging

import pytest

from databricks.labs.ucx.assessment.aws import AWSResources, AWSInstanceProfileCrawler


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    assert aws.validate_connection()

def test_get_roles_in_ip():
    roles = list(get_roles_in_instance_profile("databricks-s3-access"))
    print(roles)
    assert len(roles)==1


def test_get_policies():
    policies = list(get_policies_in_role("databricks-s3-access"))
    print(policies)
    assert len(policies)==6


def test_get_attached_policies():
    policies = list(get_attached_policies_in_role("databricks-s3-access"))
    print(policies)
    assert len(policies)==5


def test_role_policy(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    aws = AWSResources(profile)
    role_policy_actions = list(aws.get_role_policy("databricks-s3-access","databricks-s3-access"))
    print(role_policy_actions)
    assert len(role_policy_actions) == 15


def test_instance_profile_crawler(env_or_skip, ws, sql_backend, inventory_schema):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    instance_profile_crawler = AWSInstanceProfileCrawler(ws,sql_backend,inventory_schema)
    instance_profiles = instance_profile_crawler.snapshot()
    print(instance_profiles)
    assert instance_profiles






