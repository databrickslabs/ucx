import json
import logging
import re
import shutil
import subprocess
import typing
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache

from databricks.sdk.service.catalog import Privilege

logger = logging.getLogger(__name__)


@dataclass
class AWSRole:
    path: str
    role_name: str
    role_id: str
    arn: str


@dataclass
class AWSPolicyAction:
    resource_type: str
    privilege: str
    resource_path: str


@dataclass
class AWSRoleAction:
    role_arn: str
    resource_type: str
    privilege: str
    resource_path: str

    @property
    def role_name(self):
        role_match = re.match(AWSInstanceProfile.ROLE_NAME_REGEX, self.role_arn)
        return role_match.group(1)


@dataclass
class AWSInstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str | None = None

    ROLE_NAME_REGEX = r"arn:aws:iam::[0-9]+:(?:instance-profile|role)\/([a-zA-Z0-9+=,.@_-]*)$"

    @property
    def role_name(self) -> str | None:
        if self.iam_role_arn:
            arn = self.iam_role_arn
        else:
            arn = self.instance_profile_arn
        role_match = re.match(self.ROLE_NAME_REGEX, arn)
        if not role_match:
            logger.error(f"Role ARN is mismatched {self.iam_role_arn}")
            return None
        return role_match.group(1)


@lru_cache(maxsize=1024)
def run_command(command):
    logger.info(f"Invoking Command {command}")
    with subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        output, error = process.communicate()
        return process.returncode, output.decode("utf-8"), error.decode("utf-8")


class AWSResources:
    S3_ACTIONS: typing.ClassVar[set[str]] = {"s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:PutObjectAcl"}
    S3_READONLY: typing.ClassVar[str] = "s3:GetObject"
    S3_REGEX: typing.ClassVar[str] = r"arn:aws:s3:::([a-zA-Z0-9\/+=,.@_-]*)\/\*$"
    S3_PREFIX: typing.ClassVar[str] = "arn:aws:s3:::"
    S3_PATH_REGEX: typing.ClassVar[str] = r"((s3:\/\/)|(s3a:\/\/))(.*)"
    UC_MASTER_ROLES_ARN: typing.ClassVar[list[str]] = [
        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
        "arn:aws:iam::707343435239:role/unity-catalog-dev-UCMasterRole-G3MMN8SP21FO",
    ]

    def __init__(self, profile: str, command_runner: Callable[[str], tuple[int, str, str]] = run_command):
        self._profile = profile
        self._command_runner = command_runner

    def validate_connection(self):
        validate_command = f"sts get-caller-identity --profile {self._profile}"
        result = self._run_json_command(validate_command)
        if result:
            logger.info(result)
            return result
        return None

    def list_role_policies(self, role_name: str):
        list_policies_cmd = f"iam list-role-policies --profile {self._profile} --role-name {role_name}"
        policies = self._run_json_command(list_policies_cmd)
        if not policies:
            return []
        return policies.get("PolicyNames", [])

    def list_attached_policies_in_role(self, role_name: str):
        list_attached_policies_cmd = (
            f"iam list-attached-role-policies --profile {self._profile} --role-name {role_name}"
        )
        policies = self._run_json_command(list_attached_policies_cmd)
        if not policies:
            return []
        attached_policies = []
        for policy in policies.get("AttachedPolicies", []):
            attached_policies.append(policy.get("PolicyArn"))
        return attached_policies

    def list_all_uc_roles(self) -> list[AWSRole]:
        roles = self._run_json_command(f"iam list-roles --profile {self._profile}")
        uc_roles: list[AWSRole] = []
        roles = roles.get("Roles")
        if not roles:
            logger.warning("list-roles couldn't find any roles")
            return uc_roles
        for role in roles:
            policy_document = role.get("AssumeRolePolicyDocument")
            if not policy_document:
                continue
            for statement in policy_document["Statement"]:
                effect = statement.get("Effect")
                action = statement.get("Action")
                principal = statement.get("Principal")
                if not (effect and action and principal):
                    continue
                if effect != "Allow":
                    continue
                if action != "sts:AssumeRole":
                    continue
                principal = principal.get("AWS")
                if not principal:
                    continue
                if not self._is_uc_principal(principal):
                    continue
                uc_roles.append(
                    AWSRole(
                        role_id=role.get("RoleId"),
                        role_name=role.get("RoleName"),
                        arn=role.get("Arn"),
                        path=role.get("Path"),
                    )
                )

        return uc_roles

    def _is_uc_principal(self, principal):
        if isinstance(principal, list):
            for single_principal in principal:
                if single_principal in self.UC_MASTER_ROLES_ARN:
                    return True
            return False
        return principal in self.UC_MASTER_ROLES_ARN

    def get_role_policy(self, role_name, policy_name: str | None = None, attached_policy_arn: str | None = None):
        if policy_name:
            get_policy = (
                f"iam get-role-policy --profile {self._profile} --role-name {role_name} " f"--policy-name {policy_name}"
            )
        elif attached_policy_arn:
            get_attached_policy = f"iam get-policy --profile {self._profile} --policy-arn {attached_policy_arn}"
            attached_policy = self._run_json_command(get_attached_policy)
            if not attached_policy:
                return []
            policy_version = attached_policy["Policy"]["DefaultVersionId"]
            get_policy = (
                f"iam get-policy-version --profile {self._profile} --policy-arn {attached_policy_arn} "
                f"--version-id {policy_version}"
            )
        else:
            logger.error("Failed to retrieve role. No role name or attached role ARN specified.")
            return []
        policy = self._run_json_command(get_policy)
        if not policy:
            return []
        if policy_name:
            actions = policy["PolicyDocument"].get("Statement", [])
        else:
            actions = policy["PolicyVersion"]["Document"].get("Statement", [])
        return self._policy_actions(actions)

    def _policy_actions(self, actions):
        policy_actions = []
        for action in actions:
            if action.get("Effect", "Deny") != "Allow":
                continue
            actions = action["Action"]
            s3_actions = self._s3_actions(actions)
            if not s3_actions or self.S3_READONLY not in s3_actions:
                continue
            privilege = Privilege.WRITE_FILES.value
            for s3_action_type in self.S3_ACTIONS:
                if s3_action_type not in s3_actions:
                    privilege = Privilege.READ_FILES.value
                    continue
            for resource in action.get("Resource", []):
                match = re.match(self.S3_REGEX, resource)
                if match:
                    policy_actions.append(AWSPolicyAction("s3", privilege, f"s3://{match.group(1)}"))
                    policy_actions.append(AWSPolicyAction("s3", privilege, f"s3a://{match.group(1)}"))
        return policy_actions

    def _s3_actions(self, actions):
        s3_actions = []
        if isinstance(actions, list):
            for single_action in actions:
                if single_action in self.S3_ACTIONS:
                    s3_actions.append(single_action)
                    continue
        elif actions in self.S3_ACTIONS:
            s3_actions = [actions]
        return s3_actions

    def _aws_role_trust_doc(self, external_id="0000"):
        return self._get_json_for_cli(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                        },
                        "Action": "sts:AssumeRole",
                        "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
                    }
                ],
            }
        )

    def _aws_s3_policy(self, s3_prefixes, account_id, role_name, kms_key=None):
        """
        Create the UC IAM policy for the given S3 prefixes, account ID, role name, and KMS key.
        """
        s3_prefixes_strip = set()
        for path in s3_prefixes:
            match = re.match(AWSResources.S3_PATH_REGEX, path)
            if match:
                s3_prefixes_strip.add(match.group(4))

        s3_prefixes_enriched = sorted([self.S3_PREFIX + s3_prefix for s3_prefix in s3_prefixes_strip])
        statement = [
            {
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": s3_prefixes_enriched,
                "Effect": "Allow",
            },
            {
                "Action": ["sts:AssumeRole"],
                "Resource": [f"arn:aws:iam::{account_id}:role/{role_name}"],
                "Effect": "Allow",
            },
        ]
        if kms_key:
            statement.append(
                {
                    "Action": ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*"],
                    "Resource": [f"arn:aws:kms:{kms_key}"],
                    "Effect": "Allow",
                }
            )
        return self._get_json_for_cli(
            {
                "Version": "2012-10-17",
                "Statement": statement,
            }
        )

    def _create_role(self, role_name: str, assume_role_json: str) -> str | None:
        """
        Create an AWS role with the given name and assume role policy document.
        """
        add_role = self._run_json_command(
            f"iam create-role --role-name {role_name} --assume-role-policy-document {assume_role_json}"
        )
        if not add_role:
            return None
        return add_role["Role"]["Arn"]

    def create_uc_role(self, role_name: str) -> str | None:
        """
        Create an IAM role for Unity Catalog to access the S3 buckets.
        the AssumeRole condition will be modified later with the external ID captured from the UC credential.
        https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
        """
        return self._create_role(role_name, self._aws_role_trust_doc())

    def update_uc_trust_role(self, role_name: str, external_id: str = "0000") -> str | None:
        """
        Modify an existing IAM role for Unity Catalog to access the S3 buckets with the external ID
        captured from the UC credential.
        https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
        """
        update_role = self._run_json_command(
            f"iam update-assume-role-policy --role-name {role_name} --policy-document {self._aws_role_trust_doc(external_id)}"
        )
        if not update_role:
            return None
        return update_role["Role"]["Arn"]

    def put_role_policy(
        self, role_name: str, policy_name: str, s3_prefixes: set[str], account_id: str, kms_key=None
    ) -> bool:
        if not self._run_command(
            f"iam put-role-policy --role-name {role_name} "
            f"--policy-name {policy_name} "
            f"--policy-document {self._aws_s3_policy(s3_prefixes, account_id, role_name, kms_key)}"
        ):
            return False
        return True

    def create_migration_role(self, role_name: str) -> str | None:
        aws_role_trust_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }
        assume_role_json = self._get_json_for_cli(aws_role_trust_doc)
        return self._create_role(role_name, assume_role_json)

    def get_instance_profile(self, instance_profile_name: str) -> str | None:
        instance_profile = self._run_json_command(
            f"iam get-instance-profile --instance-profile-name {instance_profile_name}"
        )

        if not instance_profile:
            return None

        return instance_profile["InstanceProfile"]["Arn"]

    def create_instance_profile(self, instance_profile_name: str) -> str | None:
        instance_profile = self._run_json_command(
            f"iam create-instance-profile --instance-profile-name {instance_profile_name}"
        )

        if not instance_profile:
            return None

        return instance_profile["InstanceProfile"]["Arn"]

    def delete_instance_profile(self, instance_profile_name: str, role_name: str):
        self._run_json_command(
            f"iam remove-role-from-instance-profile --instance-profile-name {instance_profile_name}"
            f" --role-name {role_name}"
        )
        self._run_json_command(f"iam delete-instance-profile --instance-profile-name {instance_profile_name}")
        self._run_json_command(f"iam delete-role --role-name {role_name}")

    def add_role_to_instance_profile(self, instance_profile_name: str, role_name: str):
        # there can only be one role associated with iam instance profile
        self._run_command(
            f"iam add-role-to-instance-profile --instance-profile-name {instance_profile_name} --role-name {role_name}"
        )

    def role_exists(self, role_name: str) -> bool:
        """
        Check if the given role exists in the AWS account.
        """
        result = self._run_json_command("iam list-roles")
        roles = result.get("Roles", [])
        for role in roles:
            if role["RoleName"] == role_name:
                return True
        return False

    def _run_json_command(self, command: str):
        aws_cmd = shutil.which("aws")
        code, output, error = self._command_runner(f"{aws_cmd} {command} --output json")
        if code != 0:
            logger.error(error)
            return None
        if output == "":
            return {}
        return json.loads(output)

    def _run_command(self, command: str):
        aws_cmd = shutil.which("aws")
        code, _, error = self._command_runner(f"{aws_cmd} {command} --output json")
        if code != 0:
            logger.error(error)
            return False
        return True

    @staticmethod
    def _get_json_for_cli(input_json: dict) -> str:
        return json.dumps(input_json).replace('\n', '').replace(" ", "")
