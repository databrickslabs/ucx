import logging
from collections.abc import Iterable
from pathlib import PurePath

from databricks.labs.ucx.assessment.aws import AWSRoleAction
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class AWSExternalLocationsMigration:

    def __init__(
        self,
        ws: WorkspaceClient,
        external_locations: ExternalLocations,
        aws_resource_permissions: AWSResourcePermissions,
        principal_acl: PrincipalACL,
    ):
        self._ws = ws
        self._external_locations = external_locations
        self._aws_resource_permissions = aws_resource_permissions
        self._principal_acl = principal_acl

    def run(self, location_prefix="UCX_location"):
        """
        For each path find out the role that has access to it
        Find out the credential that is pointing to this path
        Create external location for the path using the credential identified
        """
        credential_dict = self._get_existing_credentials_dict()
        external_locations = self._external_locations.snapshot()
        existing_external_locations = self._ws.external_locations.list()
        existing_paths = [external_location.url for external_location in existing_external_locations]
        compatible_roles = self._aws_resource_permissions.load_uc_compatible_roles()
        missing_paths = self._identify_missing_external_locations(external_locations, existing_paths, compatible_roles)
        for path, role_arn in missing_paths:
            if role_arn not in credential_dict:
                logger.error(f"Missing credential for role {role_arn} for path {path}")
                continue
            self._ws.external_locations.create(
                self._generate_external_location_name(location_prefix),
                path,
                credential_dict[role_arn],
                skip_validation=True,
            )
        self._principal_acl.apply_location_acl()

    def _generate_external_location_name(self, location_prefix: str):
        external_location_num = 1
        existing_external_locations = self._ws.external_locations.list()
        external_location_names = [external_location.name for external_location in existing_external_locations]
        while True:
            external_location_name = f"{location_prefix}_{external_location_num}"
            if external_location_name not in external_location_names:
                break
            external_location_num += 1
        return external_location_name

    @staticmethod
    def _identify_missing_external_locations(
        external_locations: Iterable[ExternalLocation],
        existing_paths: list[str],
        compatible_roles: list[AWSRoleAction],
    ) -> set[tuple[str, str]]:
        """
        Get recommended external locations
        Get existing external locations
        Get list of paths from get_uc_compatible_roles
        Identify recommended external location paths that don't have an external location and return them
        """
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
