import json
import re
import typing
from collections.abc import Iterable
from functools import partial
from pathlib import PurePath

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist, PermissionDenied
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.compute import Policy
from databricks.sdk.service.sql import SetWorkspaceWarehouseConfigRequestSecurityPolicy

from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSResources,
    AWSRoleAction,
    logger,
    AWSUCRoleCandidate,
    AWSCredentialCandidate,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations


class AWSResourcePermissions:
    UC_ROLES_FILE_NAME: typing.ClassVar[str] = "uc_roles_access.csv"
    INSTANCE_PROFILES_FILE_NAME: typing.ClassVar[str] = "aws_instance_profile_info.csv"

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        aws_resources: AWSResources,
        external_locations: ExternalLocations,
        kms_key=None,
    ):
        self._installation = installation
        self._aws_resources = aws_resources
        self._ws = ws
        self._locations = external_locations
        self._aws_account_id = aws_resources.validate_connection().get("Account")
        self._kms_key = kms_key

    def list_uc_roles(
        self,
        *,
        single_role: bool = True,
        role_name: str = "UC_ROLE",
        policy_name: str = "UC_POLICY",
    ) -> list[AWSUCRoleCandidate]:
        """
        Get the missing paths
        Identify the S3 prefixes
        Create the roles and policies for the missing S3 prefixes
        If single_role is True, create a single role and policy for all the missing S3 prefixes
        If single_role is False, create a role and policy for each missing S3 prefix
        """
        roles: list[AWSUCRoleCandidate] = []
        missing_paths = self._identify_missing_paths()
        if len(missing_paths) == 0:
            return []
        s3_buckets = set()
        for missing_path in missing_paths:
            match = re.match(AWSResources.S3_BUCKET, missing_path)
            if match:
                s3_buckets.add(match.group(1))
        if single_role:
            roles.append(
                AWSUCRoleCandidate(self._generate_role_name(single_role, role_name, ""), policy_name, list(s3_buckets))
            )
        else:
            for s3_prefix in list(s3_buckets):
                roles.append(
                    AWSUCRoleCandidate(
                        self._generate_role_name(single_role, role_name, s3_prefix), policy_name, [s3_prefix]
                    )
                )
        return roles

    def create_uc_roles(self, roles: list[AWSUCRoleCandidate]):
        roles_created = []
        for role in roles:
            expanded_paths = set()
            for path in role.resource_paths:
                expanded_paths.add(path)
                expanded_paths.add(f"{path}/*")
            role_arn = self._aws_resources.create_uc_role(role.role_name)
            if role_arn:
                self._aws_resources.put_role_policy(
                    role.role_name,
                    role.policy_name,
                    expanded_paths,
                    self._aws_account_id,
                    self._kms_key,
                )
                roles_created.append(role)
        # We need to create a buffer between the role creation and the role update, Otherwise the update fails.
        for created_role in roles_created:
            self._aws_resources.update_uc_role(
                created_role.role_name, f"arn:aws:iam::{self._aws_account_id}:role/{created_role.role_name}"
            )
        return roles_created

    def update_uc_role(self, role_name, role_arn, external_id="0000"):
        return self._aws_resources.update_uc_role(role_name, role_arn, external_id)

    def save_uc_compatible_roles(self):
        uc_role_access = list(self._get_role_access())
        if len(uc_role_access) == 0:
            logger.warning("No mapping was generated.")
            return None
        return self._installation.save(uc_role_access, filename=self.UC_ROLES_FILE_NAME)

    def load_uc_compatible_roles(self):
        try:
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UC_ROLES_FILE_NAME)
        except ResourceDoesNotExist:
            self.save_uc_compatible_roles()
            role_actions = self._installation.load(list[AWSRoleAction], filename=self.UC_ROLES_FILE_NAME)
        return role_actions

    def save_instance_profile_permissions(self) -> str | None:
        instance_profile_access = list(self._get_instance_profiles_access())
        if len(instance_profile_access) == 0:
            logger.warning("No mapping was generated.")
            return None
        return self._installation.save(instance_profile_access, filename=self.INSTANCE_PROFILES_FILE_NAME)

    def role_exists(self, role_name: str) -> bool:
        return self._aws_resources.role_exists(role_name)

    def _get_instance_profiles(self) -> Iterable[AWSInstanceProfile]:
        instance_profiles = self._ws.instance_profiles.list()
        result_instance_profiles = []
        for instance_profile in instance_profiles:
            iam_role_arn = instance_profile.iam_role_arn
            role_match = re.match(AWSResources.ROLE_NAME_REGEX, instance_profile.instance_profile_arn)
            if role_match is not None:
                instance_profile_name = role_match.group(1)
                iam_role_arn = self._aws_resources.get_instance_profile_role_arn(instance_profile_name)
            result_instance_profiles.append(AWSInstanceProfile(instance_profile.instance_profile_arn, iam_role_arn))

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

    def _get_role_arn(self, role_name: str):
        return f"arn:aws:iam::{self._aws_account_id}:role/" + role_name

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
            matching_role = False
            for role in compatible_roles:
                if external_location.location.startswith(role.resource_path):
                    matching_role = True
                    continue
            if matching_role:
                continue
            missing_paths.add(external_location.location)
        return missing_paths

    def get_roles_to_migrate(self) -> list[AWSCredentialCandidate]:
        """
        Identify the roles that need to be migrated to UC from the UC compatible roles list.
        """
        external_locations = self._locations.snapshot()
        compatible_roles = self.load_uc_compatible_roles()
        roles: dict[str, AWSCredentialCandidate] = {}
        for external_location in external_locations:
            path = PurePath(external_location.location)
            for role in compatible_roles:
                if not (path.match(role.resource_path) or path.match(role.resource_path + "/*")):
                    continue
                if role.role_arn not in roles:
                    roles[role.role_arn] = AWSCredentialCandidate(
                        role_arn=role.role_arn, privilege=role.privilege, paths=set([external_location.location])
                    )
                    continue
                roles[role.role_arn].paths.add(external_location.location)
                if role.privilege == Privilege.WRITE_FILES.value:
                    roles[role.role_arn].privilege = Privilege.WRITE_FILES.value
        if len(roles) > 200:
            raise RuntimeWarning('Migration will breach UC limits (Storage Credentials > 200).')
        return list(roles.values())

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
    def get_iam_role_from_cluster_policy(cluster_policy_definition: str) -> str | None:
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

        self._ws.cluster_policies.edit(
            str(policy.policy_id), name=str(policy.name), definition=json.dumps(definition_dict)
        )

    def _update_sql_dac_with_instance_profile(self, iam_instance_profile: AWSInstanceProfile, prompts: Prompts):
        warehouse_config = self._ws.warehouses.get_workspace_warehouse_config()
        if warehouse_config.instance_profile_arn is not None:
            if not prompts.confirm(
                f"There is an existing instance profile {warehouse_config.instance_profile_arn} specified in the "
                f"workspace warehouse config. Do you want UCX to to update it with the uber instance profile?"
            ):
                return
        security_policy = (
            SetWorkspaceWarehouseConfigRequestSecurityPolicy(warehouse_config.security_policy.value)
            if warehouse_config.security_policy
            else SetWorkspaceWarehouseConfigRequestSecurityPolicy.NONE
        )
        self._ws.warehouses.set_workspace_warehouse_config(
            data_access_config=warehouse_config.data_access_config,
            sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
            instance_profile_arn=iam_instance_profile.instance_profile_arn,
            security_policy=security_policy,
        )

    def get_instance_profile(self, instance_profile_name: str) -> AWSInstanceProfile | None:
        instance_profile_arn = self._aws_resources.get_instance_profile_arn(instance_profile_name)

        if not instance_profile_arn:
            return None

        return AWSInstanceProfile(instance_profile_arn)

    def _create_uber_instance_profile(self, iam_role_name: str, iam_policy_name: str, s3_paths: set[str]):
        # Add role to instance profile - they have the same name
        if (
            not self._aws_resources.create_migration_role(iam_role_name)
            or not self._aws_resources.create_instance_profile(iam_role_name)
            or not self._aws_resources.add_role_to_instance_profile(iam_role_name, iam_role_name)
            or not self._aws_resources.put_role_policy(
                iam_role_name, iam_policy_name, s3_paths, self._aws_account_id, self._kms_key
            )
        ):
            self._aws_resources.delete_instance_profile(iam_role_name, iam_role_name)
            raise PermissionDenied(f"Failed to create migration role and instance profile {iam_role_name}")

    def create_uber_principal(self, prompts: Prompts):
        config = self._installation.load(WorkspaceConfig)
        s3_paths = {loc.location for loc in self._locations.snapshot()}
        if len(s3_paths) == 0:
            logger.info("No S3 paths to migrate found")
            return

        cluster_policy = self._get_cluster_policy(config.policy_id)
        iam_role_name_in_cluster_policy = self.get_iam_role_from_cluster_policy(str(cluster_policy.definition))

        iam_policy_name = f"UCX_MIGRATION_POLICY_{config.inventory_database}"
        iam_role_name = f"UCX_MIGRATION_ROLE_{config.inventory_database}"
        if iam_role_name_in_cluster_policy and self.role_exists(iam_role_name_in_cluster_policy):
            if not prompts.confirm(
                f"We have identified existing UCX migration role \"{iam_role_name_in_cluster_policy}\" "
                f"in cluster policy \"{cluster_policy.name}\". "
                f"Do you want to update the role's migration policy?"
            ):
                return
            self._aws_resources.put_role_policy(
                iam_role_name, iam_policy_name, s3_paths, self._aws_account_id, self._kms_key
            )
            logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")
            config.uber_instance_profile = iam_role_name_in_cluster_policy
            self._installation.save(config)
        elif self.role_exists(iam_role_name):
            if not prompts.confirm(
                f"We have identified existing UCX migration role \"{iam_role_name}\". "
                f"Do you want to update the role's migration iam policy "
                f"and add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
            ):
                return
        else:
            if not prompts.confirm(
                f"Do you want to create new migration role \"{iam_role_name}\" and "
                f"add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
            ):
                return
            self._create_uber_instance_profile(iam_role_name, iam_policy_name, s3_paths)
        iam_instance_profile = self.get_instance_profile(iam_role_name)
        if not iam_instance_profile:
            logger.error(f"Failed to create migration role and instance profile {iam_role_name}")
            return
        try:
            config.uber_instance_profile = iam_instance_profile.instance_profile_arn
            self._installation.save(config)
            self._update_cluster_policy_with_instance_profile(cluster_policy, iam_instance_profile)
            self._update_sql_dac_with_instance_profile(iam_instance_profile, prompts)
            logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")
        except PermissionDenied:
            logger.error(f"Failed to assign instance profile to cluster policy {iam_role_name}")
            self._aws_resources.delete_instance_profile(iam_role_name, iam_role_name)

    def _generate_role_name(self, single_role: bool, role_name: str, location: str) -> str:
        if single_role:
            metastore_id = self._ws.metastores.current().as_dict()["metastore_id"]
            return f"{role_name}_{metastore_id}"
        return f"{role_name}_{location[5:]}"

    def delete_uc_role(self, role_name: str):
        self._aws_resources.delete_role(role_name)
