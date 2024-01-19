import logging

from databricks.labs.ucx.assessment.aws import AWSPolicyAction, AWSResources

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
    role_policies = list(aws.list_role_policies("fake_role"))
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
    role_policies = list(aws.list_attached_policies_in_role("fake_role"))
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
    role_policy = list(aws.get_role_policy("fake_role", policy_name="fake_policy"))
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

    role_policy = list(aws.get_role_policy("fake_role", attached_policy_arn="arn:aws:iam::12345:policy/policy"))
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
