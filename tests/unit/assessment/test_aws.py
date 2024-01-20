import logging
from typing import BinaryIO
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.service.compute import InstanceProfile
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks.labs.ucx.assessment.aws import (
    AWSPolicyAction,
    AWSResourcePermissions,
    AWSResources,
)

logger = logging.getLogger(__name__)


def test_aws_validate():
    successful_return = """
    {
        "UserId": "uu@mail.com",
        "Account": "1234",
        "Arn": "arn:aws:sts::1234:assumed-role/AWSVIEW/uu@mail.com"
    }
    """

    def successful_call(cmd: str):
        return 0, successful_return, ""

    aws = AWSResources("Fake_Profile", successful_call)
    assert aws.validate_connection()

    def failed_call(cmd: str):
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

    def command_call(cmd: str):
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

    def command_call(cmd: str):
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
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:PutObjectAcl",
                        "s3:GetBucketNotification",
                        "s3:PutBucketNotification"
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
        if cmd.startswith("aws iam get-role-policy"):
            return 0, get_role_policy_return, ""
        elif cmd.startswith("aws iam get-policy "):
            return 0, get_policy_return, ""
        elif cmd.startswith("aws iam get-policy-version"):
            return 0, get_policy_version_return, ""
        else:
            return -1, "", "Error"

    aws = AWSResources("Fake_Profile", command_call)
    role_policy = aws.get_role_policy("fake_role", policy_name="fake_policy")
    assert role_policy == [
        AWSPolicyAction(
            resource_type="s3",
            action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
            resource_path="bucket1",
        ),
        AWSPolicyAction(
            resource_type="s3",
            action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
            resource_path="bucket2",
        ),
        AWSPolicyAction(
            resource_type="s3",
            action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
            resource_path="bucket3",
        ),
    ]

    role_policy = aws.get_role_policy("fake_role", attached_policy_arn="arn:aws:iam::12345:policy/policy")
    assert role_policy == [
        AWSPolicyAction(
            resource_type="s3",
            action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
            resource_path="bucketA",
        ),
        AWSPolicyAction(
            resource_type="s3",
            action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
            resource_path="bucketB",
        ),
        AWSPolicyAction(
            resource_type="s3",
            action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
            resource_path="bucketC",
        ),
    ]


def test_save_instance_profile_permissions():
    ws = create_autospec(WorkspaceClient)
    ws.instance_profiles.list.return_value = [
        InstanceProfile("arn:aws:iam::12345:instance-profile/role1", "arn:aws:iam::12345:role/role1")
    ]
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])

    expected_csv_entries = [
        "arn:aws:iam::12345:instance-profile/role1,s3",
        "bucket1,arn:aws:iam::12345:role/role1",
        "arn:aws:iam::12345:instance-profile/role1,s3",
        "bucket2,arn:aws:iam::12345:role/role1",
        "arn:aws:iam::12345:instance-profile/role1,s3",
        "bucket3,arn:aws:iam::12345:role/role1",
        "arn:aws:iam::12345:instance-profile/role1,s3",
        "bucketA,arn:aws:iam::12345:role/role1",
        "arn:aws:iam::12345:instance-profile/role1,s3",
        "bucketB,arn:aws:iam::12345:role/role1",
        "arn:aws:iam::12345:instance-profile/role1,s3",
        "bucketC,arn:aws:iam::12345:role/role1",
    ]

    def upload(
        path: str,
        content: BinaryIO,
        *,
        format: ImportFormat | None = None,  # noqa: A002
        language: Language | None = None,
        overwrite: bool | None = False,
    ) -> None:
        csv_text = str(content.read())
        for entry in expected_csv_entries:
            assert entry in csv_text

    ws.workspace.upload = upload
    aws = create_autospec(AWSResources)
    aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
                resource_path="bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
                resource_path="bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
                resource_path="bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
                resource_path="bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
                resource_path="bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                action={"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectAcl"},
                resource_path="bucketC",
            ),
        ],
        [],
        [],
    ]
    aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]

    aws_resource_permissions = AWSResourcePermissions(ws, aws)
    aws_resource_permissions.save_instance_profile_permissions()
