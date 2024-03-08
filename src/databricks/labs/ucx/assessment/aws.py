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
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.compute import Policy

from databricks.labs.ucx.config import WorkspaceConfig
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
    S3_REGEX: typing.ClassVar[str] = r"arn:aws:s3:::([a-zA-Z0-9+=,.@_-]*)\/\*$"
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

    def _add_role(self, role_name: str, assume_role_json: str) -> str | None:
        """
        Create an AWS role with the given name and assume role policy document.
        """
        add_role = self._run_json_command(
            f"iam create-role --role-name {role_name} --assume-role-policy-document {assume_role_json}"
        )
        if not add_role:
            return None
        return add_role["Role"]["Arn"]

    def add_uc_role(self, role_name: str) -> str | None:
        """
        Create an IAM role for Unity Catalog to access the S3 buckets.
        the AssumeRole condition will be modified later with the external ID captured from the UC credential.
        https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
        """
        return self._add_role(role_name, self._aws_role_trust_doc())

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

    def add_uc_role_policy(
        self, role_name: str, policy_name: str, s3_prefixes: set[str], account_id: str, kms_key=None
    ) -> bool:
        if not self._run_command(
            f"iam put-role-policy --role-name {role_name} "
            f"--policy-name {policy_name} "
            f"--policy-document {self._aws_s3_policy(s3_prefixes, account_id, role_name, kms_key)}"
        ):
            return False
        return True

    def add_migration_role(self, role_name: str) -> str | None:
        aws_role_trust_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }
        assume_role_json = self._get_json_for_cli(aws_role_trust_doc)
        return self._add_role(role_name, assume_role_json)

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

    def add_role_to_instance_profile(self, instance_profile_name: str, role_name: str):
        # there can only be one role associated with iam instance profile
        self._run_command(
            f"iam add-role-to-instance-profile --instance-profile-name {instance_profile_name} --role-name {role_name}"
        )

    def add_or_update_migration_policy(
        self, role_name: str, policy_name: str, s3_prefixes: set[str], account_id: str, kms_key=None
    ):
        if not self._run_command(
            f"iam put-role-policy "
            f"--role-name {role_name} --policy-name {policy_name} "
            f"--policy-document {self._aws_s3_policy(s3_prefixes, account_id, role_name, kms_key)}"
        ):
            return False
        return True

    def is_role_exists(self, role_name: str) -> bool:
        """
        Check if the given role exists in the AWS account.
        """
        result = self._run_json_command(f"iam list-roles --profile {self._profile}")
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
        external_locations: ExternalLocations,
        schema: str,
        aws_account_id=None,
        kms_key=None,
    ):
        self._installation = installation
        self._aws_resources = aws_resources
        self._backend = backend
        self._ws = ws
        self._locations = external_locations
        self._schema = schema
        self._aws_account_id = aws_account_id
        self._kms_key = kms_key
        self._filename = self.INSTANCE_PROFILES_FILE_NAMES

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, installation, backend, aws, schema, kms_key=None):
        config = installation.load(WorkspaceConfig)
        caller_identity = aws.validate_connection()
        locations = ExternalLocations(ws, backend, config.inventory_database)
        if not caller_identity:
            raise ResourceWarning("AWS CLI is not configured properly.")
        return cls(
            installation,
            ws,
            backend,
            aws,
            locations,
            schema,
            caller_identity.get("Account"),
            kms_key,
        )

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
                s3_prefixes.add(missing_path)
        if single_role:
            if self._aws_resources.add_uc_role(role_name):
                self._aws_resources.add_uc_role_policy(
                    role_name, policy_name, s3_prefixes, self._aws_account_id, self._kms_key
                )
        else:
            role_id = 1
            for s3_prefix in sorted(list(s3_prefixes)):
                if self._aws_resources.add_uc_role(f"{role_name}-{role_id}"):
                    self._aws_resources.add_uc_role_policy(
                        f"{role_name}-{role_id}",
                        f"{policy_name}-{role_id}",
                        {s3_prefix},
                        self._aws_account_id,
                        self._kms_key,
                    )
                role_id += 1

    def update_uc_role_trust_policy(self, role_name, external_id="0000"):
        return self._aws_resources.update_uc_trust_role(role_name, external_id)

    def save_uc_compatible_roles(self):
        uc_role_access = list(self._get_role_access())
        if len(uc_role_access) == 0:
            logger.warning("No mapping was generated.")
            return None
        return self._installation.save(uc_role_access, filename=self.UC_ROLES_FILE_NAMES)

    def load_uc_compatible_roles(self):
        try:
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UC_ROLES_FILE_NAMES)
        except ResourceDoesNotExist:
            self.save_uc_compatible_roles()
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UC_ROLES_FILE_NAMES)
        return role_actions

    def save_instance_profile_permissions(self) -> str | None:
        instance_profile_access = list(self._get_instance_profiles_access())
        if len(instance_profile_access) == 0:
            logger.warning("No mapping was generated.")
            return None
        return self._installation.save(instance_profile_access, filename=self.INSTANCE_PROFILES_FILE_NAMES)

    def is_role_exists(self, role_name: str) -> bool:
        return self._aws_resources.is_role_exists(role_name)

    def _get_instance_profiles(self) -> Iterable[AWSInstanceProfile]:
        instance_profiles = self._ws.instance_profiles.list()
        result_instance_profiles = []
        for instance_profile in instance_profiles:
            if not instance_profile.iam_role_arn:
                instance_profile.iam_role_arn = instance_profile.instance_profile_arn.replace(
                    "instance-profile", "role"
                )
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
        external_locations = self._locations.snapshot()
        compatible_roles = self.load_uc_compatible_roles()
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

    def _get_cluster_policy(self, policy_id: str | None) -> Policy:
        if not policy_id:
            msg = "Cluster policy not found in UCX config"
            logger.error(msg)
            raise NotFound(msg) from None
        try:
            return self._ws.cluster_policies.get(policy_id=policy_id)
        except NotFound as err:
            msg = f"UCX Policy {policy_id} not found, please reinstall UCX"
            logger.error(msg)
            raise NotFound(msg) from err

    def _get_iam_role_from_cluster_policy(self, cluster_policy_definition: str) -> str | None:
        definition_dict = json.loads(cluster_policy_definition)
        if definition_dict.get("aws_attributes.instance_profile_arn") is not None:
            instance_profile_arn = definition_dict.get("aws_attributes.instance_profile_arn").get("value")
            logger.info(f"Migration instance profile is set to {instance_profile_arn}")

            return AWSInstanceProfile(instance_profile_arn).role_name

        return None

    def _update_cluster_policy_with_instance_profile(self, policy: Policy, iam_instance_profile: AWSInstanceProfile):
        definition_dict = json.loads(str(policy.definition))
        definition_dict["aws_attributes.instance_profile_arn"] = {
            "type": "fixed",
            "value": iam_instance_profile.instance_profile_arn,
        }

        self._ws.cluster_policies.edit(str(policy.policy_id), str(policy.name), definition=json.dumps(definition_dict))

    def create_external_locations(self, location_init="UCX_location"):
        # For each path find out the role that has access to it
        # Find out the credential that is pointing to this path
        # Create external location for the path using the credential identified
        credential_dict = self._get_existing_credentials_dict()
        external_locations = self._locations.snapshot()
        existing_external_locations = self._ws.external_locations.list()
        existing_paths = [external_location.url for external_location in existing_external_locations]
        compatible_roles = self.load_uc_compatible_roles()
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

    def get_instance_profile(self, instance_profile_name: str) -> AWSInstanceProfile | None:
        instance_profile_arn = self._aws_resources.get_instance_profile(instance_profile_name)

        if not instance_profile_arn:
            return None

        return AWSInstanceProfile(instance_profile_arn)

    def create_uber_principal(self, prompts: Prompts):

        config = self._installation.load(WorkspaceConfig)

        external_locations = self._locations.snapshot()
        s3_paths = {loc.location for loc in external_locations}

        if len(s3_paths) == 0:
            logger.info("No S3 paths to migrate found")
            return

        cluster_policy = self._get_cluster_policy(config.policy_id)
        iam_role_name_in_cluster_policy = self._get_iam_role_from_cluster_policy(str(cluster_policy.definition))

        iam_policy_name = f"UCX_MIGRATION_POLICY_{config.inventory_database}"
        if iam_role_name_in_cluster_policy and self.is_role_exists(iam_role_name_in_cluster_policy):
            if not prompts.confirm(
                f"We have identified existing UCX migration role \"{iam_role_name_in_cluster_policy}\" "
                f"in cluster policy \"{cluster_policy.name}\". "
                f"Do you want to update the role's migration policy?"
            ):
                return
            self._aws_resources.add_or_update_migration_policy(
                iam_role_name_in_cluster_policy, iam_policy_name, s3_paths, self._aws_account_id, self._kms_key
            )
            logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")
            return

        iam_role_name = f"UCX_MIGRATION_ROLE_{config.inventory_database}"
        if self.is_role_exists(iam_role_name):
            if not prompts.confirm(
                f"We have identified existing UCX migration role \"{iam_role_name}\". "
                f"Do you want to update the role's migration iam policy "
                f"and add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
            ):
                return
            self._aws_resources.add_or_update_migration_policy(
                iam_role_name, iam_policy_name, s3_paths, self._aws_account_id, self._kms_key
            )
        else:
            if not prompts.confirm(
                f"Do you want to create new migration role \"{iam_role_name}\" and "
                f"add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
            ):
                return
            role_arn = self._aws_resources.add_migration_role(iam_role_name)
            if not role_arn:
                logger.error("Could not create new IAM role")
                return
            instance_profile_arn = self._aws_resources.create_instance_profile(iam_role_name)
            if not instance_profile_arn:
                logger.error("Could not create instance profile")
                return
            # Add role to instance profile - they have the same name
            self._aws_resources.add_role_to_instance_profile(iam_role_name, iam_role_name)
        iam_instance_profile = self.get_instance_profile(iam_role_name)

        if iam_instance_profile:
            self._update_cluster_policy_with_instance_profile(cluster_policy, iam_instance_profile)
            logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")
