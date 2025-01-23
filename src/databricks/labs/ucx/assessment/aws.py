import json
import logging
import re
import shutil
from typing import Any, ClassVar
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import Privilege

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.utils import run_command

logger = logging.getLogger(__name__)


class AWSResourceType(Enum):
    """
    Enum for the resource types supported by the AWS IAM policies.
    We will add values as we support more resource types.
    """

    S3 = "s3"
    GLUE = "glue"


@dataclass
class AWSRole:
    path: str
    role_name: str
    role_id: str
    arn: str


@dataclass
class AWSPolicyAction:
    resource_type: AWSResourceType
    privilege: Privilege
    resource_path: str


@dataclass
class AWSUCRoleCandidate:
    """Candidates for UC IAM roles with the paths they have access to"""

    role_name: str
    policy_name: str
    resource_paths: list[str]
    resource_type: AWSResourceType = AWSResourceType.S3


@dataclass
class AWSRoleAction:
    role_arn: str
    resource_type: AWSResourceType
    privilege: Privilege
    resource_path: str

    @property
    def role_name(self):
        role_match = re.match(AWSResources.ROLE_NAME_REGEX, self.role_arn)
        return role_match.group(1)


@dataclass
class AWSInstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str | None = None

    @property
    def role_name(self) -> str | None:
        if self.iam_role_arn:
            arn = self.iam_role_arn
        else:
            arn = self.instance_profile_arn
        role_match = re.match(AWSResources.ROLE_NAME_REGEX, arn)
        if not role_match:
            logger.error(f"Role ARN is mismatched {self.iam_role_arn}")
            return None
        return role_match.group(1)


@dataclass()
class AWSCredentialCandidate:
    role_arn: str
    privilege: Privilege
    paths: set[str] = field(default_factory=set)

    @property
    def role_name(self):
        role_match = re.match(AWSResources.ROLE_NAME_REGEX, self.role_arn)
        return role_match.group(1)


class AWSResources:
    S3_ACTIONS: ClassVar[set[str]] = {"s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:PutObjectAcl"}
    S3_READONLY: ClassVar[str] = "s3:GetObject"
    S3_REGEX: ClassVar[re.Pattern] = re.compile(r"arn:aws:s3:::([a-zA-Z0-9\/+=,.@_-]*)\/\*$")
    S3_BUCKET: ClassVar[re.Pattern] = re.compile(r"((s3:\/\/|s3a:\/\/)([a-zA-Z0-9+=,.@_-]*))(\/.*$)?")
    S3_PREFIX: ClassVar[str] = "arn:aws:s3:::"
    S3_PATH_REGEX: ClassVar[re.Pattern] = re.compile(r"((s3:\/\/)|(s3a:\/\/))(.*)")
    GLUE_REQUIRED_ACTIONS: ClassVar[set[str]] = {
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:BatchGetPartition",
        "glue:CreateDatabase",
        "glue:CreateTable",
        "glue:CreateUserDefinedFunction",
        "glue:DeleteDatabase",
        "glue:DeletePartition",
        "glue:DeleteTable",
        "glue:DeleteUserDefinedFunction",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetUserDefinedFunction",
        "glue:GetUserDefinedFunctions",
        "glue:UpdateDatabase",
        "glue:UpdatePartition",
        "glue:UpdateTable",
        "glue:UpdateUserDefinedFunction",
    }
    # Following the documentation in https://docs.databricks.com/en/archive/external-metastores/aws-glue-metastore.html
    UC_MASTER_ROLES_ARN: ClassVar[list[str]] = [
        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
        "arn:aws:iam::707343435239:role/unity-catalog-dev-UCMasterRole-G3MMN8SP21FO",
    ]
    ROLE_NAME_REGEX = r"arn:aws:iam::[0-9]+:(?:instance-profile|role)\/([a-zA-Z0-9+=,.@_-]*)$"

    def __init__(self, profile: str, command_runner: Callable[[str], tuple[int, str, str]] = run_command):
        self._profile = profile
        self._command_runner = command_runner

    def validate_connection(self):
        validate_command = "sts get-caller-identity"
        result = self._run_json_command(validate_command)
        if result:
            logger.info(result)
            return result
        return None

    def list_role_policies(self, role_name: str):
        list_policies_cmd = f"iam list-role-policies --role-name {role_name}"
        policies = self._run_json_command(list_policies_cmd)
        if not policies:
            return []
        return policies.get("PolicyNames", [])

    def list_attached_policies_in_role(self, role_name: str):
        list_attached_policies_cmd = f"iam list-attached-role-policies --role-name {role_name}"
        policies = self._run_json_command(list_attached_policies_cmd)
        if not policies:
            return []
        attached_policies = []
        for policy in policies.get("AttachedPolicies", []):
            attached_policies.append(policy.get("PolicyArn"))
        return attached_policies

    def list_all_uc_roles(self) -> list[AWSRole]:
        roles = self._run_json_command("iam list-roles")
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
            get_policy = f"iam get-role-policy --role-name {role_name} --policy-name {policy_name}"
        elif attached_policy_arn:
            get_attached_policy = f"iam get-policy --policy-arn {attached_policy_arn}"
            attached_policy = self._run_json_command(get_attached_policy)
            if not attached_policy:
                return []
            policy_version = attached_policy["Policy"]["DefaultVersionId"]
            get_policy = f"iam get-policy-version --policy-arn {attached_policy_arn} --version-id {policy_version}"
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
            policy_actions.extend(self._s3_policy_actions(action))
            policy_actions.extend(self._glue_policy_actions(action))
        return policy_actions

    def _s3_policy_actions(self, action: dict[str, Any]) -> list[AWSPolicyAction]:
        """
        Parse action extracted from an AWS IAM policy.
        It returns the S3 paths it refers to and whether it allows read only or read/write access.
        Args:
            action: dictionary with the action part of the policy document.

        Returns:
            list[AWSPolicyAction]: list of S3 policy actions

        """
        s3_policy_actions = []
        actions = action.get("Action")
        if not actions:
            return []
        if isinstance(actions, list):
            if self.S3_READONLY not in actions:
                return []
            privilege = None
            for s3_action_type in self.S3_ACTIONS:
                if s3_action_type not in actions:
                    privilege = Privilege.READ_FILES
                    break
            privilege = privilege or Privilege.WRITE_FILES
        elif actions == self.S3_READONLY:
            privilege = Privilege.READ_FILES
        else:
            return []
        for resource in action.get("Resource", []):
            match = self.S3_REGEX.match(resource)
            if match:
                s3_policy_actions.append(AWSPolicyAction(AWSResourceType.S3, privilege, f"s3://{match.group(1)}"))
                s3_policy_actions.append(AWSPolicyAction(AWSResourceType.S3, privilege, f"s3a://{match.group(1)}"))
        return s3_policy_actions

    def _glue_policy_actions(self, action: dict[str, Any]) -> list[AWSPolicyAction]:
        """
        Parse action extracted from an AWS IAM policy.
        Args:
            action: dictionary with the action part of the policy document.

        Returns:
            list[AWSPolicyAction]: list of Glue policy actions
        """
        actions = action.get("Action")
        if not actions:
            return []
        if not isinstance(actions, list):
            actions = [actions]
        if "glue:*" not in actions:
            # Check if all the required glue action are present in the role
            for required_action in self.GLUE_REQUIRED_ACTIONS:
                if required_action not in actions:
                    return []

        if "*" not in action.get("Resource", []):
            return []
        return [AWSPolicyAction(AWSResourceType.GLUE, Privilege.USAGE, "*")]

    def _aws_role_trust_doc(self, self_assume_arn: str | None = None, external_id="0000"):
        return self._get_json_for_cli(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": (
                                [
                                    "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
                                    self_assume_arn,
                                ]
                                if self_assume_arn
                                else "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                            )
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
            match = AWSResources.S3_PATH_REGEX.match(path)
            if match:
                s3_prefixes_strip.add(match.group(4))

        s3_prefixes_enriched = sorted([self.S3_PREFIX + s3_prefix for s3_prefix in s3_prefixes_strip])
        statement = [
            {
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ],
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

    def _aws_glue_policy(self, resources: set[str], account_id: str, role_name: str) -> str:
        """
        Create the UC IAM policy for the given S3 prefixes, account ID, role name, and KMS key.
        Args:
            resources: the glue resources to allow access to
            account_id: the AWS account ID
            role_name: the name of the role
        Returns:
            str: the JSON string for the IAM policy
        """

        statement = [
            {
                "Action": [
                    "glue:*",
                ],
                "Resource": list(resources),
                "Effect": "Allow",
            },
            {
                "Action": ["sts:AssumeRole"],
                "Resource": [f"arn:aws:iam::{account_id}:role/{role_name}"],
                "Effect": "Allow",
            },
        ]
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

    def _update_role(self, role_name: str, assume_role_json: str) -> str | None:
        """
        Create an AWS role with the given name and assume role policy document.
        """
        update_role = self._run_json_command(
            f"iam update-assume-role-policy --role-name {role_name} --policy-document {assume_role_json}"
        )
        return update_role

    def create_uc_role(self, role_name: str) -> str | None:
        """
        Create an IAM role for Unity Catalog to access the S3 buckets.
        the AssumeRole condition will be modified later with the external ID captured from the UC credential.
        https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
        """
        return self._create_role(role_name, self._aws_role_trust_doc())

    @retried(on=[NotFound], timeout=timedelta(seconds=30))
    def update_uc_role(self, role_name: str, role_arn: str, external_id: str = "0000") -> str | None:
        """
        Create an IAM role for Unity Catalog to access the S3 buckets.
        the AssumeRole condition will be modified later with the external ID captured from the UC credential.
        https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
        """
        result = self._update_role(role_name, self._aws_role_trust_doc(role_arn, external_id))
        logger.debug(f"Updated role {role_name} with {result}")
        if result is None:
            raise NotFound("Assume role policy not updated.")
        return result

    def put_role_policy(
        self,
        role_name: str,
        policy_name: str,
        resource_type: AWSResourceType,
        resources: set[str],
        account_id: str,
        kms_key: str | None = None,
    ) -> bool:
        """
        Create a policy for the given role with the given S3 prefixes, account ID, and KMS key.
        Args:
            role_name: the name of the role
            policy_name: the name of the policy
            resource_type: the type of the resource (s3 or glue)
            resources: s3 prefixes to allow access to
            account_id: AWS account ID
            kms_key: (optional) KMS key to be used
        """
        if resource_type == AWSResourceType.S3:
            policy_document = self._aws_s3_policy(resources, account_id, role_name, kms_key)
        elif resource_type == AWSResourceType.GLUE:
            policy_document = self._aws_glue_policy(resources, account_id, role_name)
        else:
            logger.error(f"Resource type {resource_type.value} not supported")
            return False
        if not self._run_command(
            f"iam put-role-policy --role-name {role_name} --policy-name {policy_name} "
            f"--policy-document {policy_document}"
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

    def get_instance_profile_arn(self, instance_profile_name: str) -> str | None:
        instance_profile = self._run_json_command(
            f"iam get-instance-profile --instance-profile-name {instance_profile_name}"
        )

        if not instance_profile:
            return None

        return instance_profile["InstanceProfile"]["Arn"]

    def get_instance_profile_role_arn(self, instance_profile_name: str) -> str | None:
        instance_profile = self._run_json_command(
            f"iam get-instance-profile --instance-profile-name {instance_profile_name}"
        )

        if not instance_profile:
            return None

        try:
            return instance_profile["InstanceProfile"]["Roles"][0]["Arn"]
        except (KeyError, IndexError):
            return None

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
        self.delete_role(role_name)

    def delete_role(self, role_name: str):
        role_policies = self.list_role_policies(role_name)
        for policy in role_policies:
            self._run_json_command(f"iam delete-role-policy --role-name {role_name} --policy-name {policy}")
        self._run_json_command(f"iam delete-role --role-name {role_name}")

    def add_role_to_instance_profile(self, instance_profile_name: str, role_name: str) -> bool:
        # there can only be one role associated with iam instance profile
        return self._run_command(
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
        code, output, error = self._command_runner(f"{aws_cmd} {command} --profile {self._profile} --output json")
        if code != 0:
            logger.error(error)
            return None
        if output == "":
            return {}
        return json.loads(output)

    def _run_command(self, command: str):
        aws_cmd = shutil.which("aws")
        code, _, error = self._command_runner(f"{aws_cmd} {command} --profile {self._profile} --output json")
        if code != 0:
            logger.error(error)
            return False
        return True

    @staticmethod
    def _get_json_for_cli(input_json: dict) -> str:
        return json.dumps(input_json).replace('\n', '').replace(" ", "")


@dataclass
class AWSGlue:
    """
    AWS Glue connection information
    :param aws_region: The AWS region where the Glue Metastore is located. It defaults to the AWS_Region of the metastore.
    :type aws_region: str
    :param aws_account_id: The AWS account ID of the Glue Metastore. It defaults to the AWS account ID of the instance profile.
    :type aws_account_id: str
    """

    aws_region: str
    aws_account_id: str

    @classmethod
    def get_glue_connection_info(
        cls,
        workspace: WorkspaceClient,
        config: WorkspaceConfig,
    ):
        """
        Retrieve the AWS Glue connection information from the workspace configuration.
        Args:
            workspace: the workspace client for the given workspace
            config: the UCX configuration for the workspace
        Returns:
            AWSGlue: the AWS Glue connection information
        """
        if not config.spark_conf:
            logger.info('Spark config not found')
            return None
        spark_config = config.spark_conf
        glue_metastore = spark_config.get('spark.databricks.hive.metastore.glueCatalog.enabled')
        if not glue_metastore or glue_metastore.lower() != 'true':
            logger.info('Glue Metastore not enabled')
            return None
        aws_region = spark_config.get('spark.hadoop.aws.region')
        instance_profile_arn = config.instance_profile
        if not instance_profile_arn:
            logger.info('Instance Profile not found')
            return None

        account_id_match = re.match(r"arn:aws:iam::(\d+):instance-profile/.*", instance_profile_arn)
        if not account_id_match:
            logger.error(f"Instance profile ARN is mismatched {instance_profile_arn}")
            return None
        aws_account_id = account_id_match.group(1)
        if not aws_region:
            try:
                metastore = workspace.metastores.current().metastore_id
                aws_region = workspace.metastores.get(metastore).region
                if not aws_region:
                    logger.warning("Can't retrieve glue aws region")
                    return None
            except NotFound:
                # workspace information cannot be found
                logger.warning("Can't retrieve aws region")
                return None

        return cls(aws_region, aws_account_id)

    def as_dict(self):
        return {
            "aws_region": self.aws_region,
            "aws_account_id": self.aws_account_id,
        }

    @staticmethod
    def is_glue_in_config(
        config: WorkspaceConfig,
    ) -> bool:
        """
        Check if the Glue Metastore is enabled in the Spark configuration.
        It searches for the property spark.databricks.hive.metastore.glueCatalog.enabled.
        Args:
            config: workspace configuration to scan
        Returns:
            bool: True if the Glue Metastore is enabled, False otherwise
        """
        if not config.spark_conf:
            logger.info('Spark config not found')
            return False
        spark_config = config.spark_conf
        glue_metastore = spark_config.get('spark.databricks.hive.metastore.glueCatalog.enabled')
        if not glue_metastore or glue_metastore.lower() != 'true':
            logger.info('Glue Metastore not enabled')
            return False
        return True
