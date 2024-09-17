import logging
from collections.abc import Iterable
from pathlib import PurePath, Path

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

    def run(self) -> None:
        """
        For each path find out the role that has access to it
        Find out the credential that is pointing to this path
        Create external location for the path using the credential identified
        """
        credential_dict = self._get_existing_credentials_dict()
        external_locations = self._external_locations.snapshot()
        existing_external_locations = self._ws.external_locations.list()
        existing_paths = []
        for external_location in existing_external_locations:
            if external_location.url is not None:
                existing_paths.append(external_location.url)
        compatible_roles = self._aws_resource_permissions.load_uc_compatible_roles()
        missing_paths = self._identify_missing_external_locations(external_locations, existing_paths, compatible_roles)
        for path, role_arn in missing_paths:
            if role_arn not in credential_dict:
                logger.error(f"Missing credential for role {role_arn} for path {path}")
                continue
            location_name = self._generate_external_location_name(path)
            self._ws.external_locations.create(
                location_name,
                path,
                credential_dict[role_arn],
                skip_validation=True,
            )
        self._principal_acl.apply_location_acl()

    @staticmethod
    def _generate_external_location_name(path: str) -> str:
        return "_".join(Path(path.lower()).parts[1:])

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
                path = role.resource_path
                if path.endswith("/*"):
                    path = path[:-2]
                if new_path.match(path + "/*") or new_path.match(path):
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
