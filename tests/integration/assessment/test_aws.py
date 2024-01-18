import logging

import pytest

from databricks.labs.ucx.assessment.aws import validate_connection, get_roles_in_instance_profile, get_policies_in_role, \
    get_attached_policies_in_role


def test_aws_validate(env_or_skip):
    profile = env_or_skip("AWS_DEFAULT_PROFILE")
    assert validate_connection(profile)


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


