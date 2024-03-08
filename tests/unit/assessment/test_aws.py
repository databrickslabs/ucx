import json
import logging
from unittest import mock
from unittest.mock import MagicMock, call, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import (
    AwsIamRole,
    ExternalLocationInfo,
    StorageCredentialInfo,
)
from databricks.sdk.service.compute import InstanceProfile, Policy

from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSPolicyAction,
    AWSResourcePermissions,
    AWSResources,
    AWSRole,
    AWSRoleAction,
    run_command,
)
from databricks.labs.ucx.hive_metastore import ExternalLocations
from tests.unit import DEFAULT_CONFIG
from tests.unit.framework.mocks import MockBackend

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_ws():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.instance_profiles.list.return_value = [
        InstanceProfile("arn:aws:iam::12345:instance-profile/role1", "arn:aws:iam::12345:role/role1")
    ]
    return ws


@pytest.fixture
def mock_installation():
    installation = MockInstallation(DEFAULT_CONFIG)
    installation.load = MagicMock()
    return installation


@pytest.fixture
def backend():
    rows = {
        "external_locations": [["s3://BUCKET1/FOLDER1", 1], ["s3://BUCKET2/FOLDER2", 1], ["s3://BUCKETX/FOLDERX", 1]]
    }
    return MockBackend(rows=rows, fails_on_first={})


@pytest.fixture
def mock_aws():
    return create_autospec(AWSResources)


@pytest.fixture
def locations(mock_ws, backend):
    return ExternalLocations(mock_ws, backend, "ucx")


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


def test_save_instance_profile_permissions(mock_ws, mock_installation, mock_aws, locations):
    mock_aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketC",
            ),
        ],
        [],
        [],
    ]
    mock_aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    mock_aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]

    aws_resource_permissions = AWSResourcePermissions(
        mock_installation, mock_ws, MockBackend(), mock_aws, locations, "ucx"
    )
    aws_resource_permissions.save_instance_profile_permissions()

    mock_installation.assert_file_written(
        'aws_instance_profile_info.csv',
        [
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket1',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket2',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket3',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketA',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketB',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketC',
                'resource_type': 's3',
            },
        ],
    )


def test_save_uc_compatible_roles(mock_ws, mock_installation, mock_aws, locations):
    mock_aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketC",
            ),
        ],
        [],
        [],
    ]
    mock_aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    mock_aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]
    mock_aws.list_all_uc_roles.return_value = [
        AWSRole(path='/', role_name='uc-role1', role_id='12345', arn='arn:aws:iam::12345:role/uc-role1')
    ]

    aws_resource_permissions = AWSResourcePermissions(
        mock_installation, mock_ws, MockBackend(), mock_aws, locations, "ucx"
    )
    aws_resource_permissions.save_uc_compatible_roles()
    mock_installation.assert_file_written(
        'uc_roles_access.csv',
        [
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket1',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket2',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket3',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketA',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketB',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketC',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
        ],
    )


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


def test_instance_profiles_empty_mapping(mock_ws, mock_installation, mock_aws, locations, caplog):
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation, mock_ws, MockBackend(), mock_aws, locations, "ucx"
    )
    aws_resource_permissions.save_instance_profile_permissions()
    assert 'No mapping was generated.' in caplog.messages


def test_uc_roles_empty_mapping(mock_ws, mock_installation, mock_aws, locations, caplog):
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation, mock_ws, MockBackend(), mock_aws, locations, "ucx"
    )
    aws_resource_permissions.save_uc_compatible_roles()
    assert 'No mapping was generated.' in caplog.messages


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
    aws.add_uc_role("test_role")
    assert (
        '/path/aws iam create-role --role-name test_role '
        '--assume-role-policy-document '
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":'
        '{"AWS":"arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"}'
        ',"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"0000"}}}]} --output json'
    ) in command_calls


def test_update_uc_trust_role(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    aws.update_uc_trust_role("test_role", "1234")
    assert (
        '/path/aws iam update-assume-role-policy --role-name test_role '
        '--policy-document '
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":'
        '{"AWS":"arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"}'
        ',"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"1234"}}}]} --output json'
    ) in command_calls


def test_create_uc_role_policy_no_kms(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"Role": {"Arn": "arn:aws:iam::123456789012:role/Test-Role"}}', ""

    aws = AWSResources("Fake_Profile", command_call)
    s3_prefixes = {"s3://BUCKET1/FOLDER1", "s3://BUCKET1/FOLDER1/*", "s3://BUCKET2/FOLDER2", "s3://BUCKET2/FOLDER2/*"}
    aws.add_uc_role_policy("test_role", "test_policy", s3_prefixes, "1234")
    assert (
        '/path/aws iam put-role-policy --role-name test_role '
        '--policy-name test_policy --policy-document '
        '{"Version":"2012-10-17","Statement":[{"Action":["s3:GetObject","s3:PutObject","s3:DeleteObject",'
        '"s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::BUCKET1/FOLDER1",'
        '"arn:aws:s3:::BUCKET1/FOLDER1/*","arn:aws:s3:::BUCKET2/FOLDER2","arn:aws:s3:::BUCKET2/FOLDER2/*"],'
        '"Effect":"Allow"},{"Action":["sts:AssumeRole"],"Resource":["arn:aws:iam::1234:role/test_role"],'
        '"Effect":"Allow"}]} --output json'
    ) in command_calls


def test_create_uc_role_kms(mocker):
    command_calls = []
    mocker.patch("shutil.which", return_value="/path/aws")

    def command_call(cmd: str):
        command_calls.append(cmd)
        return 0, '{"VALID":"JSON"}', ""

    aws = AWSResources("Fake_Profile", command_call)
    s3_prefixes = {"s3://BUCKET1/FOLDER1", "s3://BUCKET1/FOLDER1/*", "s3://BUCKET2/FOLDER2", "s3://BUCKET2/FOLDER2/*"}
    aws.add_uc_role_policy("test_role", "test_policy", s3_prefixes, "1234", "key_arn")
    assert (
        '/path/aws iam put-role-policy --role-name test_role '
        '--policy-name test_policy '
        '--policy-document {"Version":"2012-10-17","Statement":[{"Action":["s3:GetObject","s3:PutObject",'
        '"s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::BUCKET1/FOLDER1",'
        '"arn:aws:s3:::BUCKET1/FOLDER1/*","arn:aws:s3:::BUCKET2/FOLDER2","arn:aws:s3:::BUCKET2/FOLDER2/*"],'
        '"Effect":"Allow"},{"Action":["sts:AssumeRole"],"Resource":["arn:aws:iam::1234:role/test_role"],'
        '"Effect":"Allow"},{"Action":["kms:Decrypt","kms:Encrypt","kms:GenerateDataKey*"],'
        '"Resource":["arn:aws:kms:key_arn"],"Effect":"Allow"}]} --output json'
    ) in command_calls


def test_create_uc_role_single(mock_ws, mock_installation, mock_aws, backend, locations):
    mock_installation.load.return_value = [
        AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKETX/*")
    ]
    aws_resource_permissions = AWSResourcePermissions(mock_installation, mock_ws, backend, mock_aws, locations, "ucx")
    aws_resource_permissions.create_uc_roles_cli()
    assert mock_aws.add_uc_role.assert_called_with('UC_ROLE') is None
    assert (
        mock_aws.add_uc_role_policy.assert_called_with(
            'UC_ROLE', 'UC_POLICY', {'s3://BUCKET1/FOLDER1', 's3://BUCKET2/FOLDER2'}, None, None
        )
        is None
    )


def test_create_uc_role_multiple(mock_ws, mock_installation, mock_aws, backend, locations):
    mock_installation.load.return_value = [
        AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKETX/*")
    ]
    aws_resource_permissions = AWSResourcePermissions(mock_installation, mock_ws, backend, mock_aws, locations, "ucx")
    aws_resource_permissions.create_uc_roles_cli(single_role=False)
    assert call('UC_ROLE-1') in mock_aws.add_uc_role.call_args_list
    assert call('UC_ROLE-2') in mock_aws.add_uc_role.call_args_list
    assert (
        call('UC_ROLE-1', 'UC_POLICY-1', {'s3://BUCKET1/FOLDER1'}, None, None)
        in mock_aws.add_uc_role_policy.call_args_list
    )
    assert (
        call('UC_ROLE-2', 'UC_POLICY-2', {'s3://BUCKET2/FOLDER2'}, None, None)
        in mock_aws.add_uc_role_policy.call_args_list
    )


def test_get_uc_compatible_roles(mock_ws, mock_installation, mock_aws, locations):
    mock_aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketC",
            ),
        ],
        [],
        [],
    ]
    mock_aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    mock_aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]
    mock_aws.list_all_uc_roles.return_value = [
        AWSRole(path='/', role_name='uc-role1', role_id='12345', arn='arn:aws:iam::12345:role/uc-role1')
    ]

    aws_resource_permissions = AWSResourcePermissions(
        mock_installation, mock_ws, MockBackend(), mock_aws, locations, "ucx"
    )
    mock_installation.load = MagicMock()
    mock_installation.load.side_effect = [
        ResourceDoesNotExist(),
        [AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKETX/*")],
    ]
    aws_resource_permissions.load_uc_compatible_roles()
    mock_installation.assert_file_written(
        'uc_roles_access.csv',
        [
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket1',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket2',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket3',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketA',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketB',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketC',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
        ],
    )


def test_create_external_locations(mock_ws, mock_installation, mock_aws, backend, locations):
    mock_installation.load = MagicMock()
    mock_installation.load.return_value = [
        AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKET1/*"),
        AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKET2/*"),
        AWSRoleAction("arn:aws:iam::12345:role/uc-rolex", "s3", "WRITE_FILES", "s3://BUCKETX/FOLDERX"),
    ]
    mock_ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            id="1",
            name="cred1",
            aws_iam_role=AwsIamRole("arn:aws:iam::12345:role/uc-role1"),
        ),
        StorageCredentialInfo(
            id="2",
            name="credx",
            aws_iam_role=AwsIamRole("arn:aws:iam::12345:role/uc-rolex"),
        ),
    ]
    aws_resource_permissions = AWSResourcePermissions(mock_installation, mock_ws, backend, mock_aws, locations, "ucx")
    aws_resource_permissions.create_external_locations()
    calls = [
        call(mock.ANY, 's3://BUCKET1/FOLDER1', 'cred1'),
        call(mock.ANY, 's3://BUCKET2/FOLDER2', 'cred1'),
        call(mock.ANY, 's3://BUCKETX/FOLDERX', 'credx'),
    ]
    mock_ws.external_locations.create.assert_has_calls(calls, any_order=True)


def test_create_external_locations_skip_existing(mock_ws, mock_installation, mock_aws, backend, locations):
    mock_installation.load = MagicMock()
    mock_installation.load.return_value = [
        AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKET1/*"),
        AWSRoleAction("arn:aws:iam::12345:role/uc-rolex", "s3", "WRITE_FILES", "s3://BUCKETX/FOLDERX"),
    ]
    mock_ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            id="1",
            name="cred1",
            aws_iam_role=AwsIamRole("arn:aws:iam::12345:role/uc-role1"),
        ),
        StorageCredentialInfo(
            id="2",
            name="credx",
            aws_iam_role=AwsIamRole("arn:aws:iam::12345:role/uc-rolex"),
        ),
    ]
    mock_ws.external_locations.list.return_value = [
        ExternalLocationInfo(name="UCX_FOO_1", url="s3://BUCKETX/FOLDERX", credential_name="credx"),
    ]

    aws_resource_permissions = AWSResourcePermissions(mock_installation, mock_ws, backend, mock_aws, locations, "ucx")
    aws_resource_permissions.create_external_locations(location_init="UCX_FOO")
    calls = [
        call("UCX_FOO_2", 's3://BUCKET1/FOLDER1', 'cred1'),
    ]
    mock_ws.external_locations.create.assert_has_calls(calls, any_order=True)


def test_create_uber_principal(mock_ws, mock_installation, mock_aws, backend, locations):
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    mock_ws.cluster_policies.get.return_value = cluster_policy
    instance_profile_arn = "arn:aws:iam::12345:instance-profile/role1"
    mock_aws.get_instance_profile.return_value = instance_profile_arn
    locations = ExternalLocations(mock_ws, backend, "ucx")
    prompts = MockPrompts({"We have identified existing UCX migration role *": "yes"})
    aws_resource_permissions = AWSResourcePermissions(mock_installation, mock_ws, backend, mock_aws, locations, "ucx")
    aws_resource_permissions.create_uber_principal(prompts)
    definition = {"foo": "bar", "aws_attributes.instance_profile_arn": {"type": "fixed", "value": instance_profile_arn}}
    mock_ws.cluster_policies.edit.assert_called_with(
        'foo', 'Unity Catalog Migration (ucx) (me@example.com)', definition=json.dumps(definition)
    )


def test_create_uber_principal_no_storage(mock_ws, mock_installation, mock_aws, locations):
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    mock_ws.cluster_policies.get.return_value = cluster_policy
    instance_profile_arn = "arn:aws:iam::12345:instance-profile/role1"
    mock_aws.get_instance_profile.return_value = instance_profile_arn
    locations = ExternalLocations(mock_ws, MockBackend(), "ucx")
    prompts = MockPrompts({})
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation, mock_ws, MockBackend(), mock_aws, locations, "ucx"
    )
    assert not aws_resource_permissions.create_uber_principal(prompts)
