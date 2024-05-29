import logging

import pytest

from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSPolicyAction,
    AWSResources,
    AWSRole,
)
from databricks.labs.ucx.framework.utils import run_command

logger = logging.getLogger(__name__)


def test_aws_validate():
    successful_return = """
    {
        "UserId": "uu@mail.com",
        "Account": "1234",
        "Arn": "arn:aws:sts::1234:assumed-role/AWSVIEW/uu@mail.com"
    }
    """

    def successful_call(_):
        return 0, successful_return, ""

    aws = AWSResources("Fake_Profile", successful_call)
    assert aws.validate_connection()

    def failed_call(_):
        return -1, "", "Can't connect"

    aws = AWSResources("Fake_Profile", failed_call)
    assert not aws.validate_connection()


def test_list_role_policies():
    command_return = """
{
    "PolicyNames": [
        "Policy1",
        "Policy2",
        "Policy3"
    ],
    "IsTruncated": false
}

    """

    def command_call(_):
        return 0, command_return, ""

    aws = AWSResources("Fake_Profile", command_call)
    role_policies = aws.list_role_policies("fake_role")
    assert role_policies == ["Policy1", "Policy2", "Policy3"]


def test_list_attached_policies_in_role():
    command_return = """
{
    "AttachedPolicies": [
        {
            "PolicyName": "Policy1",
            "PolicyArn": "arn:aws:iam::aws:policy/Policy1"
        },
        {
            "PolicyName": "Policy2",
            "PolicyArn": "arn:aws:iam::aws:policy/Policy2"
        }
    ],
    "IsTruncated": false
}

    """

    def command_call(_):
        return 0, command_return, ""

    aws = AWSResources("Fake_Profile", command_call)
    role_policies = aws.list_attached_policies_in_role("fake_role")
    assert role_policies == ["arn:aws:iam::aws:policy/Policy1", "arn:aws:iam::aws:policy/Policy2"]


def test_get_role_policy():
    get_role_policy_return = """
{
    "RoleName": "Role",
    "PolicyName": "Policy",
    "PolicyDocument": {
        "Version": "2024-01-01",
        "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject"
                    ],
                    "Resource": [
                        "arn:aws:s3:::bucket1/*",
                        "arn:aws:s3:::bucket2/*",
                        "arn:aws:s3:::bucket3/*"
                    ]
                }
        ]
    }
}

    """

    get_policy_return = """
{
    "Policy": {
        "PolicyName": "policy",
        "PolicyId": "1234",
        "Arn": "arn:aws:iam::12345:policy/policy",
        "Path": "/",
        "DefaultVersionId": "v1",
        "AttachmentCount": 1,
        "PermissionsBoundaryUsageCount": 0,
        "IsAttachable": true,
        "Description": "Policy Description",
        "CreateDate": "2024-01-01T00:00:00+00:00",
        "UpdateDate": "2024-01-01T00:00:00+00:00",
        "Tags": []
    }
}
    """

    get_policy_version_return = """
{
    "PolicyVersion": {
        "Document": {
            "Version": "V1",
            "Statement": [
                {
                    "Sid": "Permit",
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:PutObjectAcl",
                        "s3:GetBucketNotification",
                        "s3:PutBucketNotification"
                    ],
                    "Resource": [
                        "arn:aws:s3:::bucketA/*",
                        "arn:aws:s3:::bucketB/*",
                        "arn:aws:s3:::bucketC/*"
                    ]
                }
            ]
        },
        "VersionId": "v1",
        "IsDefaultVersion": true,
        "CreateDate": "2024-01-01T00:00:00+00:00"
    }
}
    """

    def command_call(cmd: str):
        if "iam get-role-policy" in cmd:
            return 0, get_role_policy_return, ""
        if "iam get-policy " in cmd:
            return 0, get_policy_return, ""
        if "iam get-policy-version" in cmd:
            return 0, get_policy_version_return, ""
        return -1, "", "Error"

    aws = AWSResources("Fake_Profile", command_call)
    role_policy = aws.get_role_policy("fake_role", policy_name="fake_policy")
    assert role_policy == [
        AWSPolicyAction(
            resource_type="s3",
            privilege="READ_FILES",
            resource_path="s3://bucket1",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="READ_FILES",
            resource_path="s3a://bucket1",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="READ_FILES",
            resource_path="s3://bucket2",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="READ_FILES",
            resource_path="s3a://bucket2",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="READ_FILES",
            resource_path="s3://bucket3",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="READ_FILES",
            resource_path="s3a://bucket3",
        ),
    ]

    role_policy = aws.get_role_policy("fake_role", attached_policy_arn="arn:aws:iam::12345:policy/policy")
    assert role_policy == [
        AWSPolicyAction(
            resource_type="s3",
            privilege="WRITE_FILES",
            resource_path="s3://bucketA",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="WRITE_FILES",
            resource_path="s3a://bucketA",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="WRITE_FILES",
            resource_path="s3://bucketB",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="WRITE_FILES",
            resource_path="s3a://bucketB",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="WRITE_FILES",
            resource_path="s3://bucketC",
        ),
        AWSPolicyAction(
            resource_type="s3",
            privilege="WRITE_FILES",
            resource_path="s3a://bucketC",
        ),
    ]


def test_get_uc_roles():
    list_roles_return = """
    {
        "Roles": [
            {
                "Path": "/",
                "RoleName": "NON-UC-Role",
                "RoleId": "12345",
                "Arn": "arn:aws:iam::123456789:role/non-uc-role",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "ec2.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "MaxSessionDuration": 3600
            },
            {
                "Path": "/",
                "RoleName": "uc-role-1",
                "RoleId": "12345",
                "Arn": "arn:aws:iam::123456789:role/uc-role-1",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": [
                                    "arn:aws:iam::123456789:role/uc-role",
                                    "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                                ]
                            },
                            "Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334455"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Path": "/",
                "RoleName": "uc-role-2",
                "RoleId": "123456",
                "Arn": "arn:aws:iam::123456789:role/uc-role-2",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                            },
                            "Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334466"
                                }
                            }
                        }
                    ]
                }
            }
        ]
    }
    """

    def command_call(_):
        return 0, list_roles_return, ""

    aws = AWSResources("Fake_Profile", command_call)
    uc_roles = aws.list_all_uc_roles()
    assert uc_roles == [
        AWSRole(path='/', role_name='uc-role-1', role_id='12345', arn='arn:aws:iam::123456789:role/uc-role-1'),
        AWSRole(path='/', role_name='uc-role-2', role_id='123456', arn='arn:aws:iam::123456789:role/uc-role-2'),
    ]


def test_get_uc_roles_missing_keys():
    list_roles_return = """
    {
        "Roles": [
            {
                "Path": "/",
                "RoleName": "NON-UC-Role",
                "RoleId": "12345",
                "Arn": "arn:aws:iam::123456789:role/non-uc-role",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Defect": "Allow",
                            "Principal": {
                                "Service": "ec2.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "MaxSessionDuration": 3600
            },
            {
                "Path": "/",
                "RoleName": "uc-role-1",
                "RoleId": "12345",
                "Arn": "arn:aws:iam::123456789:role/uc-role-1",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": [
                                    "arn:aws:iam::123456789:role/uc-role",
                                    "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                                ]
                            },
                            "Non-Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334455"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Path": "/",
                "RoleName": "uc-role-2",
                "RoleId": "123456",
                "Arn": "arn:aws:iam::123456789:role/uc-role-2",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                            },
                            "Action": "sts:DontAssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334466"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Path": "/",
                "RoleName": "uc-role-3",
                "RoleId": "123456",
                "Arn": "arn:aws:iam::123456789:role/uc-role-3",
                "CreateDate": "2024-01-01T00:00:00+00:00"

            },
            {
                "Path": "/",
                "RoleName": "uc-role-4",
                "RoleId": "123456",
                "Arn": "arn:aws:iam::123456789:role/uc-role-4",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": {
                                "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                            },
                            "Action": "sts:DontAssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334466"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Path": "/",
                "RoleName": "uc-role-5",
                "RoleId": "12345",
                "Arn": "arn:aws:iam::123456789:role/uc-role-5",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": [
                                    "arn:aws:iam::123456789:role/uc-role-5",
                                    "arn:aws:iam::123456789:role/another-role"
                                ]
                            },
                            "Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334455"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Path": "/",
                "RoleName": "uc-role-6",
                "RoleId": "12345",
                "Arn": "arn:aws:iam::123456789:role/uc-role-6",
                "CreateDate": "2024-01-01T00:00:00+00:00",
                "AssumeRolePolicyDocument": {
                    "Version": "2024-01-01",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": "arn:aws:iam::123456789:role/uc-role-5"
                            },
                            "Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "1122334455"
                                }
                            }
                        }
                    ]
                }
            }
        ]
    }
    """

    def command_call(_):
        return 0, list_roles_return, ""

    aws = AWSResources("Fake_Profile", command_call)
    uc_roles = aws.list_all_uc_roles()
    assert not uc_roles


def test_role_mismatched(caplog):
    instance_profile = AWSInstanceProfile("test", "fake")
    assert not instance_profile.role_name
    assert "Role ARN is mismatched" in caplog.messages[0]


def test_get_role_policy_missing_role(caplog):
    def command_call(_):
        return 0, "", ""

    aws = AWSResources("Fake_Profile", command_call)

    aws.get_role_policy("fake_role")
    assert "No role name or attached role ARN specified." in caplog.messages[0]


def test_command(caplog):
    return_code, _, _ = run_command("echo success")
    assert return_code == 0
    with pytest.raises(FileNotFoundError) as exception:
        run_command("no_way_this_command_would_work")
        print(exception)


def test_create_uc_role(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.create_uc_role("test_role")
    assert (
        '/path/aws iam create-role --role-name test_role '
        '--assume-role-policy-document '
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":'
        '{"AWS":"arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"}'
        ',"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"0000"}}}]} --profile Fake_Profile --output json'
    ) in command_calls


def test_update_uc_trust_role_append(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        if "iam get-role" in cmd:
            return (
                0,
                """
{
    "Role": {
        "Path": "/",
        "RoleName": "Test-Role",
        "RoleId": "ABCD",
        "Arn": "arn:aws:iam::0123456789:role/Test-Role",
        "CreateDate": "2024-01-01T12:00:00+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::0123456789:root"
                    },
                    "Action": "sts:AssumeRole"
                },
                {
                    "Effect":"Allow",
                    "Principal": {
                        "AWS":"arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                    },
                    "Action":"sts:AssumeRole",
                    "Condition":{"StringEquals":{"sts:ExternalId":"00000"}}
                }
            ]
        },
        "Description": "",
        "MaxSessionDuration": 3600,
        "RoleLastUsed": {}
    }
}
            """,
                "",
            )
        command_calls.append(cmd)
        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.update_uc_role("test_role", "arn:aws:iam::123456789012:role/Test-Role", "1234")
    assert (
        '/path/aws iam update-assume-role-policy --role-name test_role '
        '--policy-document '
        '{"Version":"2012-10-17",'
        '"Statement":[{"Effect":"Allow","Principal":'
        '{"AWS":["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",'
        '"arn:aws:iam::123456789012:role/Test-Role"]},'
        '"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"1234"}}}]} '
        '--profile Fake_Profile --output json'
    ) in command_calls


def test_update_uc_trust_role(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        if "iam get-role" in cmd:
            return (
                0,
                """
{
    "Role": {
        "Path": "/",
        "RoleName": "Test-Role",
        "RoleId": "ABCD",
        "Arn": "arn:aws:iam::0123456789:role/Test-Role",
        "CreateDate": "2024-01-01T12:00:00+00:00",
        "Description": "",
        "MaxSessionDuration": 3600,
        "RoleLastUsed": {}
    }
}
            """,
                "",
            )
        command_calls.append(cmd)
        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.update_uc_role("test_role", "arn:aws:iam::123456789012:role/Test-Role", "1234")
    assert (
        '/path/aws iam update-assume-role-policy --role-name test_role '
        '--policy-document '
        '{"Version":"2012-10-17",'
        '"Statement":['
        '{"Effect":"Allow",'
        '"Principal":{"AWS":'
        '["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",'
        '"arn:aws:iam::123456789012:role/Test-Role"]},'
        '"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"1234"}}}]} '
        '--profile Fake_Profile --output json'
    ) in command_calls


def test_create_uc_role_policy_no_kms(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    s3_prefixes = {"s3://BUCKET1/FOLDER1", "s3://BUCKET1/FOLDER1/*", "s3://BUCKET2/FOLDER2", "s3://BUCKET2/FOLDER2/*"}
    aws.put_role_policy("test_role", "test_policy", s3_prefixes, "1234")
    assert (
        '/path/aws iam put-role-policy --role-name test_role '
        '--policy-name test_policy --policy-document '
        '{"Version":"2012-10-17","Statement":[{"Action":["s3:GetObject","s3:PutObject","s3:PutObjectAcl",'
        '"s3:DeleteObject",'
        '"s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::BUCKET1/FOLDER1",'
        '"arn:aws:s3:::BUCKET1/FOLDER1/*","arn:aws:s3:::BUCKET2/FOLDER2","arn:aws:s3:::BUCKET2/FOLDER2/*"],'
        '"Effect":"Allow"},{"Action":["sts:AssumeRole"],"Resource":["arn:aws:iam::1234:role/test_role"],'
        '"Effect":"Allow"}]} --profile Fake_Profile --output json'
    ) in command_calls


def test_create_uc_role_kms(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"VALID":"JSON"}', ""

    aws = AWSResources("Fake_Profile", command_call)
    s3_prefixes = {"s3://BUCKET1/FOLDER1", "s3://BUCKET1/FOLDER1/*", "s3://BUCKET2/FOLDER2", "s3://BUCKET2/FOLDER2/*"}
    aws.put_role_policy("test_role", "test_policy", s3_prefixes, "1234", "key_arn")
    assert (
        '/path/aws iam put-role-policy --role-name test_role '
        '--policy-name test_policy '
        '--policy-document {"Version":"2012-10-17","Statement":[{"Action":["s3:GetObject","s3:PutObject",'
        '"s3:PutObjectAcl",'
        '"s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::BUCKET1/FOLDER1",'
        '"arn:aws:s3:::BUCKET1/FOLDER1/*","arn:aws:s3:::BUCKET2/FOLDER2","arn:aws:s3:::BUCKET2/FOLDER2/*"],'
        '"Effect":"Allow"},{"Action":["sts:AssumeRole"],"Resource":["arn:aws:iam::1234:role/test_role"],'
        '"Effect":"Allow"},{"Action":["kms:Decrypt","kms:Encrypt","kms:GenerateDataKey*"],'
        '"Resource":["arn:aws:kms:key_arn"],"Effect":"Allow"}]} --profile Fake_Profile --output json'
    ) in command_calls


def test_create_instance_profile(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"InstanceProfile": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.create_instance_profile("test_profile")
    assert (
        '/path/aws iam create-instance-profile --instance-profile-name test_profile --profile Fake_Profile --output json'
        in command_calls
    )

    def failed_call(_):
        return -1, "", "Can't connect"

    aws = AWSResources("Fake_Profile", failed_call)
    assert not aws.create_instance_profile("test_profile")


def test_delete_instance_profile(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.delete_instance_profile("test_profile", "test_profile")
    assert (
        '/path/aws iam delete-instance-profile --instance-profile-name test_profile --profile Fake_Profile --output json'
        in command_calls
    )
    assert '/path/aws iam delete-role --role-name test_profile --profile Fake_Profile --output json' in command_calls
    assert (
        '/path/aws iam remove-role-from-instance-profile '
        '--instance-profile-name test_profile --role-name test_profile --profile Fake_Profile --output json'
    ) in command_calls


def test_role_exists(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def empty_call(cmd: str):
        command_calls.append(cmd)
        return 0, '', ""

    aws = AWSResources("Fake_Profile", empty_call)
    assert aws.role_exists("test_profile") is False
    assert '/path/aws iam list-roles --profile Fake_Profile --output json' in command_calls

    def exists_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"Roles": [{"RoleName": "test_profile"}]}', ""

    aws = AWSResources("Fake_Profile", exists_call)
    assert aws.role_exists("test_profile") is True
    assert '/path/aws iam list-roles --profile Fake_Profile --output json' in command_calls


def test_update_uc_role_append(mocker):
    """
    Test that the update_uc_role method appends the required self assume role.

    """
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        if "iam get-role" in cmd:
            return (
                0,
                """
{
    "Role": {
        "Path": "/",
        "RoleName": "Test-Role",
        "RoleId": "ABCD",
        "Arn": "arn:aws:iam::0123456789:role/Test-Role",
        "CreateDate": "2024-01-01T12:00:00+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::0123456789:root"
                    },
                    "Action": "sts:AssumeRole"
                },
                {
                    "Effect":"Allow",
                    "Principal": {
                        "AWS":"arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                    },
                    "Action":"sts:AssumeRole",
                    "Condition":{"StringEquals":{"sts:ExternalId":"00000"}}
                }
            ]
        },
        "Description": "",
        "MaxSessionDuration": 3600,
        "RoleLastUsed": {}
    }
}
            """,
                "",
            )

        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.update_uc_role("test_role", "arn:aws:iam::0123456789:role/Test-Role")
    assert (
        '/path/aws iam update-assume-role-policy --role-name test_role --policy-document '
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":'
        '{"AWS":["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",'
        '"arn:aws:iam::0123456789:role/Test-Role"]},"Action":"sts:AssumeRole",'
        '"Condition":{"StringEquals":{"sts:ExternalId":"0000"}}}]} --profile Fake_Profile --output json'
    ) in command_calls
