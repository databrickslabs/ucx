import json
import logging
import re
import shutil
import subprocess
import typing
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import lru_cache, partial
from pathlib import PurePath

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.catalog import Privilege, StorageCredentialInfo

from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import ExternalLocations

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


@dataclass
class AWSInstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str | None = None

    ROLE_NAME_REGEX = r"arn:aws:iam::[0-9]+:(?:instance-profile|role)\/([a-zA-Z0-9+=,.@_-]*)$"

    @property
    def role_name(self):
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


def esc_json_for_cli(json: str) -> str:
    return json.replace('"', '\\"').replace('\n', '\\n')


class AWSResources:
    S3_ACTIONS: typing.ClassVar[set[str]] = {"s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:PutObjectAcl"}
    S3_READONLY: typing.ClassVar[str] = "s3:GetObject"
    S3_REGEX: typing.ClassVar[str] = r"arn:aws:s3:::([a-zA-Z0-9+=,.@_-]*)\/\*$"
    S3_PREFIX: typing.ClassVar[str] = "arn:aws:s3:::"
    UC_MASTER_ROLES_ARN: typing.ClassVar[list[str]] = [
        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
        "arn:aws:iam::707343435239:role/unity-catalog-dev-UCMasterRole-G3MMN8SP21FO",
    ]
    AWS_ROLE_TRUST_DOC: typing.ClassVar[str] = """
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "0000"
                }
            }
        }
    ]
}
    """

    AWS_POLICY_KMS: typing.ClassVar[str] = """
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Action": [
              "s3:GetObject",
              "s3:PutObject",
              "s3:DeleteObject",
              "s3:ListBucket",
              "s3:GetBucketLocation"
          ],
          "Resource": <BUCKETS>,
          "Effect": "Allow"
      },
      {
          "Action": [
              "kms:Decrypt",
              "kms:Encrypt",
              "kms:GenerateDataKey*"
          ],
          "Resource": [
              "arn:aws:kms:<KMS-KEY>"
          ],
          "Effect": "Allow"
      },
      {
          "Action": [
              "sts:AssumeRole"
          ],
          "Resource": [
              "arn:aws:iam::<AWS-ACCOUNT-ID>:role/<AWS-IAM-ROLE-NAME>"
          ],
          "Effect": "Allow"
      }
    ]
}    
    """

    AWS_POLICY_NO_KMS: typing.ClassVar[str] = """
{
      "Version": "2012-10-17",
      "Statement": [
          {
              "Action": [
                  "s3:GetObject",
                  "s3:PutObject",
                  "s3:DeleteObject",
                  "s3:ListBucket",
                  "s3:GetBucketLocation"
              ],
              "Resource": <BUCKETS>,
              "Effect": "Allow"
          },
          {
              "Action": [
                  "sts:AssumeRole"
              ],
              "Resource": [
                  "arn:aws:iam::<AWS-ACCOUNT-ID>:role/<AWS-IAM-ROLE-NAME>"
              ],
              "Effect": "Allow"
          }
        ]
    }    
        """

    SELF_ASSUME_ROLE_POLICY: typing.ClassVar[str] = """
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
          "arn:aws:iam::<AWS-ACCOUNT-ID>:role/<AWS-IAM-ROLE-NAME>"
        ]
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE-CREDENTIAL-EXTERNAL-ID>"
        }
      }
    }
  ]
}    
    """

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

    def list_all_uc_roles(self):
        roles = self._run_json_command(f"iam list-roles --profile {self._profile}")
        uc_roles = []
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
                if isinstance(principal, list):
                    is_uc_principal = False
                    for single_principal in principal:
                        if single_principal in self.UC_MASTER_ROLES_ARN:
                            is_uc_principal = True
                            continue
                    if not is_uc_principal:
                        continue
                elif principal not in self.UC_MASTER_ROLES_ARN:
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
        policy_actions = []
        for action in actions:
            if action.get("Effect", "Deny") != "Allow":
                continue
            actions = action["Action"]
            s3_action = []
            if isinstance(actions, list):
                for single_action in actions:
                    if single_action in self.S3_ACTIONS:
                        s3_action.append(single_action)
                        continue
            elif actions in self.S3_ACTIONS:
                s3_action = [actions]

            if not s3_action or self.S3_READONLY not in s3_action:
                continue
            privilege = Privilege.WRITE_FILES.value
            for s3_action_type in self.S3_ACTIONS:
                if s3_action_type not in s3_action:
                    privilege = Privilege.READ_FILES.value
                    continue

            for resource in action.get("Resource", []):
                match = re.match(self.S3_REGEX, resource)
                if match:
                    policy_actions.append(AWSPolicyAction("s3", privilege, f"s3://{match.group(1)}"))
                    policy_actions.append(AWSPolicyAction("s3", privilege, f"s3a://{match.group(1)}"))
        return policy_actions

    def add_uc_role(self, role_name, policy_name, s3_prefixes: set[str], account_id: str, kms_key=None):
        assume_role_json = esc_json_for_cli(self.AWS_ROLE_TRUST_DOC)
        add_role = self._run_json_command(
            f"iam create-role --role-name {role_name} --assume-role-policy-document {assume_role_json}")
        if not add_role:
            return False
        if kms_key:
            policy_document = self.AWS_POLICY_KMS
        else:
            policy_document = self.AWS_POLICY_NO_KMS
        s3_prefixes_enriched = {self.S3_PREFIX+s3_prefix for s3_prefix in s3_prefixes}
        policy_document = policy_document.replace("<BUCKET>",json.dumps(s3_prefixes_enriched))
        policy_document = policy_document.replace("<AWS-ACCOUNT-ID>",account_id)
        policy_document = policy_document.replace("<AWS-IAM-ROLE-NAME>", role_name)
        policy_document_json = esc_json_for_cli(policy_document)
        add_policy = self._run_json_command(
            f"iam put-role-policy --role-name {role_name} --policy-name {policy_name} --policy-document {policy_document_json}")
        if not add_policy:
            return False

    def _run_json_command(self, command: str):
        aws_cmd = shutil.which("aws")
        code, output, error = self._command_runner(f"{aws_cmd} {command} --output json")
        if code != 0:
            logger.error(error)
            return None
        return json.loads(output)


class AWSResourcePermissions:
    UCRolesFileName: typing.ClassVar[str] = "uc_roles_access.csv"
    InstanceProfilesFileName: typing.ClassVar[str] = "aws_instance_profile_info.csv"

    def __init__(self, installation: Installation, ws: WorkspaceClient, backend: StatementExecutionBackend,
                 aws_resources: AWSResources, schema: str, aws_account_id=None, kms_key=None):
        self._installation = installation
        self._aws_resources = aws_resources
        self._backend = backend
        self._ws = ws
        self._schema = schema
        self._aws_account_id = aws_account_id
        self._kms_key = kms_key

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, backend, aws_profile, schema, kms_key=None, product='ucx'):
        installation = Installation.current(ws, product)
        aws = AWSResources(aws_profile)
        caller_identity = aws.validate_connection()
        if not caller_identity:
            raise ResourceWarning("AWS CLI is not configured properly.")
        return cls(installation, ws, backend, aws,
                   schema=schema, aws_account_id=caller_identity.get("Account"), kms_key=kms_key)

    def save_uc_compatible_roles(self):
        uc_role_access = list(self._get_role_access())
        if len(uc_role_access) == 0:
            logger.warning("No Mapping Was Generated.")
            return None
        return self._installation.save(uc_role_access, filename=self.UCRolesFileName)

    def get_uc_compatible_roles(self):
        try:
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UCRolesFileName)
        except ResourceDoesNotExist:
            self.save_uc_compatible_roles()
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UCRolesFileName)
        return role_actions

    def get_uc_missing_roles(self, default_role_arn):
        missing_paths = self._identify_missing_paths()
        role_actions = []
        for path in missing_paths:
            role_actions.append(AWSRoleAction(default_role_arn, "s3", "Privilege.WRITE_FILES.value", path))
        return role_actions

    def create_uc_roles_cli(self, *, single_role=True, single_role_name=None):
        missing_paths = self._identify_missing_paths()
        if single_role:
            self._aws_resources.add_uc_role(single_role_name, missing_paths, self._aws_account_id, self._kms_key)
        else:
            for missing_path in missing_paths:
                self._aws_resources.add_uc_role(single_role_name, {missing_path}, self._aws_account_id, self._kms_key)

    def _get_instance_profiles(self) -> Iterable[AWSInstanceProfile]:
        instance_profiles = self._ws.instance_profiles.list()
        result_instance_profiles = []
        for instance_profile in instance_profiles:
            result_instance_profiles.append(
                AWSInstanceProfile(instance_profile.instance_profile_arn, instance_profile.iam_role_arn)
            )

        return result_instance_profiles

    def _get_instance_profiles_access(self):
        instance_profiles = list(self._get_instance_profiles())
        tasks = []
        for instance_profile in instance_profiles:
            tasks.append(
                partial(self._get_role_access_task, instance_profile.instance_profile_arn, instance_profile.role_name)
            )
        # Aggregating the outputs from all the tasks
        return sum(Threads.strict("Scanning Instance Profiles", tasks), [])

    def _get_role_access(self):
        roles = list(self._aws_resources.list_all_uc_roles())
        tasks = []
        for role in roles:
            tasks.append(partial(self._get_role_access_task, role.arn, role.role_name))
        # Aggregating the outputs from all the tasks
        return sum(Threads.strict("Scanning Roles", tasks), [])

    def _get_role_access_task(self, arn: str, role_name: str):
        policy_actions = []
        policies = list(self._aws_resources.list_role_policies(role_name))
        for policy in policies:
            actions = self._aws_resources.get_role_policy(role_name, policy_name=policy)
            for action in actions:
                policy_actions.append(
                    AWSRoleAction(
                        arn,
                        action.resource_type,
                        action.privilege,
                        action.resource_path,
                    )
                )
        attached_policies = self._aws_resources.list_attached_policies_in_role(role_name)
        for attached_policy in attached_policies:
            actions = list(self._aws_resources.get_role_policy(role_name, attached_policy_arn=attached_policy))
            for action in actions:
                policy_actions.append(
                    AWSRoleAction(
                        arn,
                        action.resource_type,
                        action.privilege,
                        action.resource_path,
                    )
                )
        return policy_actions

    def _identify_missing_paths(self):
        external_locations = ExternalLocations(self._ws, self._backend, self._schema).snapshot()
        compatible_roles = self.get_uc_compatible_roles()
        missing_paths = set()
        for external_location in external_locations:
            path = PurePath(external_location.location)
            matching_role = False
            for role in compatible_roles:
                if path.match(role.resource_path):
                    matching_role = True
                    continue
            if matching_role:
                continue
            missing_paths.add(external_location.location)
        return missing_paths

    def save_instance_profile_permissions(self) -> str | None:
        instance_profile_access = list(self._get_instance_profiles_access())
        if len(instance_profile_access) == 0:
            logger.warning("No Mapping Was Generated.")
            return None
        return self._installation.save(instance_profile_access, filename=self.InstanceProfilesFileName)


