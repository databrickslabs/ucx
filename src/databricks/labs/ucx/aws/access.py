import json
import re
import typing
from collections.abc import Iterable
from functools import partial
from pathlib import PurePath

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.compute import Policy

from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSResources,
    AWSRoleAction,
    logger,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation


class AWSResourcePermissions:
    UC_ROLES_FILE_NAMES: typing.ClassVar[str] = "uc_roles_access.csv"
    INSTANCE_PROFILES_FILE_NAMES: typing.ClassVar[str] = "aws_instance_profile_info.csv"

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        backend: SqlBackend,
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
            if self._aws_resources.create_uc_role(role_name):
                self._aws_resources.put_role_policy(
                    role_name, policy_name, s3_prefixes, self._aws_account_id, self._kms_key
                )
        else:
            role_id = 1
            for s3_prefix in sorted(list(s3_prefixes)):
                if self._aws_resources.create_uc_role(f"{role_name}-{role_id}"):
                    self._aws_resources.put_role_policy(
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

    def role_exists(self, role_name: str) -> bool:
        return self._aws_resources.role_exists(role_name)

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

    @staticmethod
    def _identify_missing_external_locations(
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
                if new_path.match(role.resource_path + "/*"):
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

    @staticmethod
    def _get_iam_role_from_cluster_policy(cluster_policy_definition: str) -> str | None:
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
            self._ws.external_locations.create(
                external_location_name, path, credential_dict[role_arn], skip_validation=True
            )
            external_location_num += 1

    def get_instance_profile(self, instance_profile_name: str) -> AWSInstanceProfile | None:
        instance_profile_arn = self._aws_resources.get_instance_profile(instance_profile_name)

        if not instance_profile_arn:
            return None

        return AWSInstanceProfile(instance_profile_arn)

    def _create_uber_instance_profile(self, iam_role_name):
        self._aws_resources.create_migration_role(iam_role_name)
        self._aws_resources.create_instance_profile(iam_role_name)
        # Add role to instance profile - they have the same name
        self._aws_resources.add_role_to_instance_profile(iam_role_name, iam_role_name)

    def create_uber_principal(self, prompts: Prompts):

        config = self._installation.load(WorkspaceConfig)

        s3_paths = {loc.location for loc in self._locations.snapshot()}

        if len(s3_paths) == 0:
            logger.info("No S3 paths to migrate found")
            return

        cluster_policy = self._get_cluster_policy(config.policy_id)
        iam_role_name_in_cluster_policy = self._get_iam_role_from_cluster_policy(str(cluster_policy.definition))

        iam_policy_name = f"UCX_MIGRATION_POLICY_{config.inventory_database}"
        if iam_role_name_in_cluster_policy and self.role_exists(iam_role_name_in_cluster_policy):
            if not prompts.confirm(
                f"We have identified existing UCX migration role \"{iam_role_name_in_cluster_policy}\" "
                f"in cluster policy \"{cluster_policy.name}\". "
                f"Do you want to update the role's migration policy?"
            ):
                return
            self._aws_resources.put_role_policy(
                iam_role_name_in_cluster_policy, iam_policy_name, s3_paths, self._aws_account_id, self._kms_key
            )
            logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")
            config.uber_instance_profile = iam_role_name_in_cluster_policy
            self._installation.save(config)
            return

        iam_role_name = f"UCX_MIGRATION_ROLE_{config.inventory_database}"
        if self.role_exists(iam_role_name):
            if not prompts.confirm(
                f"We have identified existing UCX migration role \"{iam_role_name}\". "
                f"Do you want to update the role's migration iam policy "
                f"and add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
            ):
                return
            self._aws_resources.put_role_policy(
                iam_role_name, iam_policy_name, s3_paths, self._aws_account_id, self._kms_key
            )
        else:
            if not prompts.confirm(
                f"Do you want to create new migration role \"{iam_role_name}\" and "
                f"add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
            ):
                return
            self._create_uber_instance_profile(iam_role_name)
        iam_instance_profile = self.get_instance_profile(iam_role_name)
        if not iam_instance_profile:
            return
        try:
            config.uber_instance_profile = iam_instance_profile.instance_profile_arn
            self._installation.save(config)
            self._update_cluster_policy_with_instance_profile(cluster_policy, iam_instance_profile)
            logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")
        except PermissionError:
            self._aws_resources.delete_instance_profile(iam_role_name, iam_role_name)
