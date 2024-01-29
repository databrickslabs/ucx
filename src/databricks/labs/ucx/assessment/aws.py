import csv
import dataclasses
import io
import json
import logging
import re
import shutil
import subprocess
import typing
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import lru_cache, partial

from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.workspace import ImportFormat

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
class AWSInstanceProfileAction:
    instance_profile_arn: str
    resource_type: str
    privilege: str
    resource_path: str
    iam_role_arn: str | None = None


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


class AWSResources:
    S3_ACTIONS: typing.ClassVar[set[str]] = {"s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:PutObjectAcl"}
    S3_READONLY: typing.ClassVar[str] = "s3:GetObject"
    S3_REGEX: typing.ClassVar[str] = r"arn:aws:s3:::([a-zA-Z0-9+=,.@_-]*)\/\*$"

    def __init__(self, profile: str, command_runner: Callable[[str], tuple[int, str, str]] = run_command):
        self._profile = profile
        self._command_runner = command_runner

    def validate_connection(self):
        validate_command = f"sts get-caller-identity --profile {self._profile}"
        result = self._run_json_command(validate_command)
        if result:
            logger.info(result)
            return True
        return False

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

    def get_role_policy(self, role_name, policy_name: str | None = None, attached_policy_arn: str | None = None):
        if policy_name:
            get_policy = (
                f"iam get-role-policy --profile {self._profile} --role-name {role_name} "
                f"--policy-name {policy_name} --no-paginate"
            )
        elif attached_policy_arn:
            get_attached_policy = f"iam get-policy --profile {self._profile} --policy-arn {attached_policy_arn}"
            attached_policy = self._run_json_command(get_attached_policy)
            if not attached_policy:
                return []
            policy_version = attached_policy["Policy"]["DefaultVersionId"]
            get_policy = (
                f"iam get-policy-version --profile {self._profile} --policy-arn {attached_policy_arn} "
                f"--version-id {policy_version} --no-paginate"
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

    def _run_json_command(self, command: str):
        aws_cmd = shutil.which("aws")
        code, output, error = self._command_runner(f"{aws_cmd} {command} --output json --no-paginate")
        if code != 0:
            logger.error(error)
            return None
        return json.loads(output)


class AWSResourcePermissions:
    def __init__(
        self,
        ws: WorkspaceClient,
        aws_resources: AWSResources,
        folder: str | None = None,
    ):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._folder = folder
        self._aws_resources = aws_resources
        self._ws = ws
        self._field_names = [_.name for _ in dataclasses.fields(AWSInstanceProfileAction)]

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
            tasks.append(partial(self._get_instance_profile_access_task, instance_profile))
        # Aggregating the outputs from all the tasks
        return sum(Threads.strict("Scanning Instance Profiles", tasks), [])

    def _get_instance_profile_access_task(self, instance_profile: AWSInstanceProfile):
        policy_actions = []
        policies = list(self._aws_resources.list_role_policies(instance_profile.role_name))
        for policy in policies:
            actions = self._aws_resources.get_role_policy(instance_profile.role_name, policy_name=policy)
            for action in actions:
                policy_actions.append(
                    AWSInstanceProfileAction(
                        instance_profile.instance_profile_arn,
                        action.resource_type,
                        action.privilege,
                        action.resource_path,
                        instance_profile.iam_role_arn,
                    )
                )
        attached_policies = self._aws_resources.list_attached_policies_in_role(instance_profile.role_name)
        for attached_policy in attached_policies:
            actions = list(
                self._aws_resources.get_role_policy(instance_profile.role_name, attached_policy_arn=attached_policy)
            )
            for action in actions:
                policy_actions.append(
                    AWSInstanceProfileAction(
                        instance_profile.instance_profile_arn,
                        action.resource_type,
                        action.privilege,
                        action.resource_path,
                        instance_profile.iam_role_arn,
                    )
                )
        return policy_actions

    def save_instance_profile_permissions(self) -> str | None:
        instance_profile_access = list(self._get_instance_profiles_access())
        if len(instance_profile_access) == 0:
            logger.warning("No Mapping Was Generated.")
            return None
        return self._save(instance_profile_access)

    def _overwrite_mapping(self, buffer) -> str:
        path = f"{self._folder}/aws_instance_profile_info.csv"
        self._ws.workspace.upload(path, buffer, overwrite=True, format=ImportFormat.AUTO)
        return path

    def _save(self, instance_profile_actions: list[AWSInstanceProfileAction]) -> str:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, self._field_names)
        writer.writeheader()
        for instance_profile_action in instance_profile_actions:
            writer.writerow(dataclasses.asdict(instance_profile_action))
        buffer.seek(0)
        return self._overwrite_mapping(buffer)
