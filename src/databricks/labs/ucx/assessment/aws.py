import logging
import re
import subprocess
import json
from dataclasses import dataclass
from typing import Callable, Iterable, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import InstanceProfile

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

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
    Action: set[str]
    resource_path: str


@dataclass
class AWSInstanceProfileAccess:
    instance_profile_arn: str
    iam_role_arn: str


@dataclass
class AWSInstanceProfile:
    instance_profile: str
    resource_type: str
    resource_path: str
    actions: str


def iam_profiles():
    # List all IAM roles
    roles_command = "aws iam list-roles"
    code, roles_output, roles_error = run_command(roles_command)

    if code == 0:
        roles_data = json.loads(roles_output)
        roles = roles_data.get("Roles", [])

        for role in roles:
            role_name = role["RoleName"]
            print(f"IAM Role: {role_name}")

            # List attached policies
            attached_policies_command = f"aws iam list-attached-role-policies --role-name {role_name}"
            code, attached_policies_output, attached_policies_error = run_command(attached_policies_command)

            if code == 0:
                attached_policies_data = json.loads(attached_policies_output)
                attached_policies = attached_policies_data.get("AttachedPolicies", [])
                for policy in attached_policies:
                    policy_name = policy["PolicyName"]
                    print(f"  Attached Policy: {policy_name}")

            # List inline policies
            inline_policies_command = f"aws iam list-role-policies --role-name {role_name}"
            code, inline_policies_output, inline_policies_error = run_command(inline_policies_command)

            if code == 0:
                inline_policies_data = json.loads(inline_policies_output)
                inline_policies = inline_policies_data.get("PolicyNames", [])
                for inline_policy in inline_policies:
                    print(f"  Inline Policy: {inline_policy}")

            # List S3 bucket permissions for the role
            s3_permissions_command = f"aws s3api list-buckets"
            code, s3_permissions_output, s3_permissions_error = run_command(s3_permissions_command)

            if code == 0:
                s3_buckets_data = json.loads(s3_permissions_output)
                s3_buckets = s3_buckets_data.get("Buckets", [])
                for bucket in s3_buckets:
                    bucket_name = bucket["Name"]
                    print(f"    S3 Bucket: {bucket_name}")

                    # Check if the role has access to the bucket
                    check_access_command = f"aws s3api get-bucket-policy-status --bucket {bucket_name}"
                    code, access_output, access_error = run_command(check_access_command)

                    if code == 0:
                        access_data = json.loads(access_output)
                        if access_data.get("IsPublic"):
                            print("      Access: Public")
                        else:
                            print("      Access: Restricted")
                    else:
                        print("      Access: Unknown (Error checking access)")

            print("\n")
    else:
        print("Error listing IAM roles.")
        print(roles_error)


def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return process.returncode, output.decode("utf-8"), error.decode("utf-8")


class AWSResources:

    S3_ACTIONS = {
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:PutObjectAcl"
    }
    S3_REGEX = "arn:aws:s3:::([a-zA-Z0-9+=,.@_-]*)\/\*"

    def __init__(self, profile: str, command_runner: Callable[[str],str] = run_command):
        self._profile = profile
        self._command_runner = command_runner

    def validate_connection(self):
        validate_command = f"aws sts get-caller-identity --profile {self._profile}"
        code, output, error = run_command(validate_command)
        if code == 0:
            logger.info(output)
            return True
        logger.error(error)
        return False

    def get_roles_in_instance_profile(self, instance_profile_name: str):
        get_instance_profile_cmd = f"aws iam get-instance-profile --profile {self._profile} --instance-profile-name {instance_profile_name}"
        code, output, error = run_command(get_instance_profile_cmd)
        if code == 0:
            ip_details = json.loads(output)
            try:
                for role in ip_details["InstanceProfile"]["Roles"]:
                    yield AWSRole(
                        path=role["Path"],
                        role_name=role["RoleName"],
                        role_id=role["RoleId"],
                        arn=role["Arn"]
                    )
            except KeyError:
                logger.error(f"Malformed response from AWS CLI {ip_details}")
        logger.error(error)

    def list_policies_in_role(self, role_name: str):
        list_policies_cmd = f"aws iam list-role-policies --profile {self._profile} --role-name {role_name} --no-paginate"
        code, output, error = run_command(list_policies_cmd)
        if code == 0:
            policies = json.loads(output)
            for policy in policies.get("PolicyNames", []):
                yield policy
        logger.error(error)

    def list_attached_policies_in_role(self, role_name: str):
        list_attached_policies_cmd = f"aws iam list-attached-role-policies --profile {self._profile} --role-name {role_name}"
        code, output, error = run_command(list_attached_policies_cmd)
        if code == 0:
            policies = json.loads(output)
            for policy in policies.get("AttachedPolicies", []):
                yield policy.get("PolicyName")
        logger.error(error)

    def get_role_policy(self, role_name, policy_name: str):
        get_policy = f"aws iam get-role-policy --profile {self._profile} --role-name {role_name} --policy-name {policy_name}"
        code, output, error = run_command(get_policy)
        if code == 0:
            policy = json.loads(output)
            for action in policy["PolicyDocument"].get("Statement", []):
                actions = action["Action"]
                s3_action = False
                if isinstance(actions, List):
                    for single_action in actions:
                        if single_action in self.S3_ACTIONS:
                            s3_action = True
                            continue
                else:
                    if actions in self.S3_ACTIONS:
                        s3_action = True

                if not s3_action:
                    continue
                for resource in action.get("Resource", []):
                    match = re.match(self.S3_REGEX, resource)
                    if match:
                        yield AWSPolicyAction("s3", set(actions,), match.group(1))
        logger.error(error)


class AWSInstanceProfileCrawler(CrawlerBase[InstanceProfile]):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "aws_instance_profile", InstanceProfile)
        self._ws = ws
        self._schema = schema

    def _crawl(self) -> Iterable[InstanceProfile]:
        return list(self._ws.instance_profiles.list())

    def snapshot(self) -> Iterable[InstanceProfile]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[InstanceProfile]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield InstanceProfile(*row)

class AWSResourcePermissions:
    def __init__(self, ws: WorkspaceClient, awsrm: AWSResources,
                 instance_profile_crawler: AWSInstanceProfileCrawler,
                 folder: str | None = None):
        self._folder = folder
        self._awsrm = awsrm
        self._ws = ws

    def _get_instance_profiles(self) -> list[str]:
        instance_profiles = self._awsrm.snapshot()



