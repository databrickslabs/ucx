import csv
import dataclasses
import io
import logging
import re
import subprocess
import json
from dataclasses import dataclass
from typing import Callable, Iterable, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import InstanceProfile
from databricks.sdk.service.workspace import ImportFormat

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
    action: set[str]
    resource_path: str


@dataclass
class AWSInstanceProfileAction:
    instance_profile_arn: str
    resource_type: str
    actions: str
    resource_path: str
    iam_role_arn: str|None = None


@dataclass
class AWSInstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str | None = None

    ROLE_NAME_REGEX = "arn:aws:iam::[0-9]+:(?:instance-profile|role)\/([a-zA-Z0-9+=,.@_-]*)$"

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
        else:
            return role_match.group(1)


def run_command(command):
    logger.info(f"Invoking Command {command}")
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

    def __init__(self, profile: str, command_runner: Callable[[str], str] = run_command):
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

    def list_role_policies(self, role_name: str):
        list_policies_cmd = f"aws iam list-role-policies --profile {self._profile} --role-name {role_name} --no-paginate"
        code, output, error = run_command(list_policies_cmd)
        if code != 0:
            logger.error(error)
            return []
        policies = json.loads(output)
        for policy in policies.get("PolicyNames", []):
            yield policy

    def list_attached_policies_in_role(self, role_name: str):
        list_attached_policies_cmd = f"aws iam list-attached-role-policies --profile {self._profile} --role-name {role_name} --no-paginate"
        code, output, error = run_command(list_attached_policies_cmd)
        if code != 0:
            logger.error(error)
            return []
        policies = json.loads(output)
        for policy in policies.get("AttachedPolicies", []):
            yield policy.get("PolicyName")

    def get_role_policy(self, role_name, policy_name: str):
        get_policy = f"aws iam get-role-policy --profile {self._profile} --role-name {role_name} --policy-name {policy_name} --no-paginate"
        code, output, error = run_command(get_policy)
        if code != 0:
            logger.error(error)
            return []
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
                    yield AWSPolicyAction("s3", set(actions, ), match.group(1))



class AWSInstanceProfileCrawler(CrawlerBase[AWSInstanceProfile]):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "aws_instance_profile", AWSInstanceProfile)
        self._ws = ws
        self._schema = schema

    def _crawl(self) -> Iterable[AWSInstanceProfile]:
        instance_profiles = self._ws.instance_profiles.list()
        for instance_profile in instance_profiles:
            yield AWSInstanceProfile(instance_profile.instance_profile_arn, instance_profile.iam_role_arn)

    def snapshot(self) -> Iterable[AWSInstanceProfile]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[InstanceProfile]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield InstanceProfile(*row)


class AWSResourcePermissions:
    def __init__(self, ws: WorkspaceClient, awsrm: AWSResources,
                 instance_profile_crawler: AWSInstanceProfileCrawler,
                 folder: str | None = None):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._folder = folder
        self._awsrm = awsrm
        self._instance_profile_crawler = instance_profile_crawler
        self._ws = ws
        self._field_names = [_.name for _ in dataclasses.fields(AWSInstanceProfileAction)]

    def _get_instance_profiles(self) -> Iterable[AWSInstanceProfile]:
        return self._instance_profile_crawler.snapshot()

    def _get_s3_access(self):
        valid_roles = self._get_instance_profiles()
        for role in valid_roles:
            policies = list(self._awsrm.list_role_policies(role.role_name))
            for policy in policies:
                actions = list(self._awsrm.get_role_policy(role.role_name, policy))
                for action in actions:
                    yield AWSInstanceProfileAction(role.instance_profile_arn,
                                                   action.resource_type,
                                                   str(action.action),
                                                   action.resource_path,
                                                   role.iam_role_arn)

    def save_instance_profile_permissions(self) -> str | None:
        instance_profile_access = list(self._get_s3_access())
        if len(instance_profile_access) == 0:
            logger.warning(
                "No Mapping Was Generated. "
                "Please check if assessment job is run"
            )
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
