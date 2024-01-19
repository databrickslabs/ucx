import csv
import dataclasses
import io
import json
import logging
import re
import subprocess
import typing
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import lru_cache, partial

from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
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
        else:
            return role_match.group(1)


@lru_cache(maxsize=1024)
def run_command(command):
    logger.info(f"Invoking Command {command}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    return process.returncode, output.decode("utf-8"), error.decode("utf-8")


class AWSResources:
    S3_ACTIONS: typing.ClassVar[set[str]] = {"s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:PutObjectAcl"}
    S3_REGEX: typing.ClassVar[str] = r"arn:aws:s3:::([a-zA-Z0-9+=,.@_-]*)\/\*"

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
        list_policies_cmd = (
            f"aws iam list-role-policies --profile {self._profile} --role-name {role_name} --no-paginate"
        )
        code, output, error = run_command(list_policies_cmd)
        if code != 0:
            logger.error(error)
            return []
        policies = json.loads(output)
        yield from policies.get("PolicyNames", [])

    def list_attached_policies_in_role(self, role_name: str):
        list_attached_policies_cmd = (
            f"aws iam list-attached-role-policies --profile {self._profile} --role-name {role_name} --no-paginate"
        )
        code, output, error = run_command(list_attached_policies_cmd)
        if code != 0:
            logger.error(error)
            return []
        policies = json.loads(output)
        for policy in policies.get("AttachedPolicies", []):
            yield policy.get("PolicyArn")

    def get_role_policy(self, role_name, policy_name: str | None = None, attached_policy_arn: str | None = None):
        if policy_name:
            get_policy = (
                f"aws iam get-role-policy --profile {self._profile} --role-name {role_name} "
                f"--policy-name {policy_name} --no-paginate"
            )
        elif attached_policy_arn:
            get_attached_policy = (
                f"aws iam get-policy --profile {self._profile} --policy-arn {attached_policy_arn} --no-paginate"
            )
            code, output, error = run_command(get_attached_policy)
            if code != 0:
                logger.error(error)
                return []
            policy_version = json.loads(output)["Policy"]["DefaultVersionId"]
            get_policy = (
                f"aws iam get-policy-version --profile {self._profile} --policy-arn {attached_policy_arn} "
                f"--version-id {policy_version} --no-paginate"
            )
        else:
            logger.error("Failed to retrieve role. No role name or attached role ARN specified.")
            return []
        code, output, error = run_command(get_policy)
        if code != 0:
            logger.error(error)
            return []
        policy = json.loads(output)
        if policy_name:
            actions = policy["PolicyDocument"].get("Statement", [])
        else:
            actions = policy["PolicyVersion"]["Document"].get("Statement", [])
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

            if not s3_action:
                continue
            for resource in action.get("Resource", []):
                match = re.match(self.S3_REGEX, resource)
                if match:
                    yield AWSPolicyAction("s3", set(s3_action), match.group(1))


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

    def _try_fetch(self) -> Iterable[AWSInstanceProfile]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AWSInstanceProfile(*row)


class AWSResourcePermissions:
    def __init__(
        self,
        ws: WorkspaceClient,
        awsrm: AWSResources,
        instance_profile_crawler: AWSInstanceProfileCrawler,
        folder: str | None = None,
    ):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._folder = folder
        self._awsrm = awsrm
        self._instance_profile_crawler = instance_profile_crawler
        self._ws = ws
        self._field_names = [_.name for _ in dataclasses.fields(AWSInstanceProfileAction)]

    def _get_instance_profiles(self) -> Iterable[AWSInstanceProfile]:
        return self._instance_profile_crawler.snapshot()

    def _get_instance_profiles_access(self):
        instance_profiles = self._get_instance_profiles()
        tasks = []
        for instance_profile in instance_profiles:
            tasks.append(partial(self._get_instance_profile_access_task, instance_profile))

        return sum(Threads.strict("Scanning Instance Profiles", tasks), [])

    def _get_instance_profile_access_task(self, instance_profile: AWSInstanceProfile):
        return list(self._get_instance_profile_access(instance_profile))

    def _get_instance_profile_access(self, instance_profile: AWSInstanceProfile):
        policies = list(self._awsrm.list_role_policies(instance_profile.role_name))
        for policy in policies:
            actions = list(self._awsrm.get_role_policy(instance_profile.role_name, policy_name=policy))
            for action in actions:
                yield AWSInstanceProfileAction(
                    instance_profile.instance_profile_arn,
                    action.resource_type,
                    str(action.action),
                    action.resource_path,
                    instance_profile.iam_role_arn,
                )
        attached_policies = list(self._awsrm.list_attached_policies_in_role(instance_profile.role_name))
        for attached_policy in attached_policies:
            actions = list(self._awsrm.get_role_policy(instance_profile.role_name, attached_policy_arn=attached_policy))
            for action in actions:
                yield AWSInstanceProfileAction(
                    instance_profile.instance_profile_arn,
                    action.resource_type,
                    str(action.action),
                    action.resource_path,
                    instance_profile.iam_role_arn,
                )

    def save_instance_profile_permissions(self) -> str | None:
        instance_profile_access = list(self._get_instance_profiles_access())
        if len(instance_profile_access) == 0:
            logger.warning("No Mapping Was Generated. Please check if assessment job is run")
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
