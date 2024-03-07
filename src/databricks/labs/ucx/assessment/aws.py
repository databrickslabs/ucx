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
from databricks.sdk.service.catalog import Privilege

from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation

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

    def add_uc_role(self, role_name):
        aws_role_trust_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
                    },
                    "Action": "sts:AssumeRole",
                    "Condition": {"StringEquals": {"sts:ExternalId": "0000"}},
                }
            ],
        }
        # the AssumeRole condition will be modified with the external ID captured from the UC credential.
        # https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
        assume_role_json = self._get_json_for_cli(aws_role_trust_doc)
        add_role = self._run_json_command(
            f"iam create-role --role-name {role_name} --assume-role-policy-document {assume_role_json}"
        )
        if not add_role:
            return False
        return True

    def add_uc_role_policy(self, role_name, policy_name, s3_prefixes: set[str], account_id: str, kms_key=None):
        s3_prefixes_enriched = sorted(
            [f"{self.S3_PREFIX}{s3_prefix}" for s3_prefix in s3_prefixes]
            + [f"{self.S3_PREFIX}{s3_prefix}/*" for s3_prefix in s3_prefixes]
        )
        statement = [
            {
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
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
        policy_document = {
            "Version": "2012-10-17",
            "Statement": statement,
        }

        policy_document_json = self._get_json_for_cli(policy_document)
        if not self._run_command(
            f"iam put-role-policy --role-name {role_name} --policy-name {policy_name} --policy-document {policy_document_json}"
        ):
            return False
        return True

    def _run_json_command(self, command: str):
        aws_cmd = shutil.which("aws")
        code, output, error = self._command_runner(f"{aws_cmd} {command} --output json")
        if code != 0:
            logger.error(error)
            return None
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


class AWSResourcePermissions:
    UC_ROLES_FILE_NAMES: typing.ClassVar[str] = "uc_roles_access.csv"
    INSTANCE_PROFILES_FILE_NAMES: typing.ClassVar[str] = "aws_instance_profile_info.csv"

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        backend: StatementExecutionBackend,
        aws_resources: AWSResources,
        schema: str,
        aws_account_id=None,
        kms_key=None,
    ):
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
        return cls(
            installation,
            ws,
            backend,
            aws,
            schema=schema,
            aws_account_id=caller_identity.get("Account"),
            kms_key=kms_key,
        )

    def save_uc_compatible_roles(self):
        uc_role_access = list(self._get_role_access())
        if len(uc_role_access) == 0:
            logger.warning("No Mapping Was Generated.")
            return None
        return self._installation.save(uc_role_access, filename=self.UC_ROLES_FILE_NAMES)

    def get_uc_compatible_roles(self):
        try:
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UC_ROLES_FILE_NAMES)
        except ResourceDoesNotExist:
            self.save_uc_compatible_roles()
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UC_ROLES_FILE_NAMES)
        return role_actions

    def create_uc_roles_cli(self, *, single_role=True, role_name="UC_ROLE", policy_name="UC_POLICY"):
        # Get the missing paths
        # Identify the S3 prefixes
        # Create the roles and policies for the missing S3 prefixes
        # If single_role is True, create a single role and policy for all the missing S3 prefixes
        # If single_role is False, create a role and policy for each missing S3 prefix
        missing_paths = self._identify_missing_paths()
        s3_prefixes = set()
        for missing_path in missing_paths:
            match = re.match(AWSResources.S3_PATH_REGEX, missing_path)
            if match:
                s3_prefixes.add(match.group(4))
        if single_role:
            if self._aws_resources.add_uc_role(role_name):
                self._aws_resources.add_uc_role_policy(
                    role_name, policy_name, s3_prefixes, account_id=self._aws_account_id, kms_key=self._kms_key
                )
        else:
            role_id = 1
            for s3_prefix in sorted(list(s3_prefixes)):
                if self._aws_resources.add_uc_role(f"{role_name}-{role_id}"):
                    self._aws_resources.add_uc_role_policy(
                        f"{role_name}-{role_id}",
                        f"{policy_name}-{role_id}",
                        {s3_prefix},
                        account_id=self._aws_account_id,
                        kms_key=self._kms_key,
                    )
                role_id += 1

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
        return self._installation.save(instance_profile_access, filename=self.INSTANCE_PROFILES_FILE_NAMES)

    def _identify_missing_external_locations(
        self,
        external_locations: Iterable[ExternalLocation],
        existing_paths: list[str],
        compatible_roles: list[AWSRoleAction],
    ) -> set[tuple[str, str]]:
        # Get recommended external locations
        # Get existing external locations
        # Get list of paths from get_uc_compatible_roles
        # Identify recommended external location paths that don't have an external location and return them
        missing_paths = set()
        for external_location in external_locations:
            existing = False
            for path in existing_paths:
                if path in external_location.location:
                    existing = True
                    continue
            if existing:
                continue
            new_path = PurePath(external_location.location)
            matching_role = None
            for role in compatible_roles:
                if new_path.match(role.resource_path):
                    matching_role = role.role_arn
                    continue
            if matching_role:
                missing_paths.add((external_location.location, matching_role))

        return missing_paths

    def _get_existing_credentials_dict(self):
        credentials = self._ws.storage_credentials.list()
        credentials_dict = {}
        for credential in credentials:
            credentials_dict[credential.aws_iam_role.role_arn] = credential.name
        return credentials_dict

    def create_external_locations(self, location_init="UCX_location"):
        # For each path find out the role that has access to it
        # Find out the credential that is pointing to this path
        # Create external location for the path using the credential identified
        credential_dict = self._get_existing_credentials_dict()
        external_locations = ExternalLocations(self._ws, self._backend, self._schema).snapshot()
        existing_external_locations = list(self._ws.external_locations.list())
        existing_paths = [external_location.url for external_location in existing_external_locations]
        compatible_roles = self.get_uc_compatible_roles()
        missing_paths = self._identify_missing_external_locations(external_locations, existing_paths, compatible_roles)
        external_location_names = [external_location.name for external_location in existing_external_locations]
        external_location_num = 1
        for path, role_arn in missing_paths:
            if role_arn not in credential_dict:
                logger.error(f"Missing credential for role {role_arn} for path {path}")
                continue
            while True:
                external_location_name = f"{location_init}_{external_location_num}"
                if external_location_name not in external_location_names:
                    break
                external_location_num += 1
            self._ws.external_locations.create(external_location_name, path, credential_dict[role_arn])
            external_location_num += 1
