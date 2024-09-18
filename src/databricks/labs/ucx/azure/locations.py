import logging
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import InvalidParameterValue, PermissionDenied
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL


logger = logging.getLogger(__name__)


class ExternalLocationsMigration:
    def __init__(
        self,
        ws: WorkspaceClient,
        hms_locations: ExternalLocations,
        resource_permissions: AzureResourcePermissions,
        azurerm: AzureResources,
        principal_acl: PrincipalACL,
    ):
        self._ws = ws
        self._hms_locations = hms_locations
        self._resource_permissions = resource_permissions
        self._azurerm = azurerm
        self._principal_acl = principal_acl

    def _app_id_credential_name_mapping(self) -> tuple[dict[str, str], dict[str, str]]:
        # list all storage credentials.
        # generate the managed identity/service principal application id to credential name mapping.
        # return one mapping for all non read-only credentials and one mapping for all read-only credentials
        # TODO: considering put this logic into the StorageCredentialManager
        app_id_mapping_write = {}
        app_id_mapping_read = {}
        all_credentials = self._ws.storage_credentials.list(max_results=0)
        for credential in all_credentials:
            name = credential.name
            # cannot have none credential name, it's required for external location
            if not name:
                continue

            read_only = credential.read_only
            service_principal = credential.azure_service_principal
            managed_identity = credential.azure_managed_identity

            application_id = None
            if service_principal:
                # if service principal based credential, use service principal's application_id directly
                application_id = service_principal.application_id
            if managed_identity:
                # if managed identity based credential, fetch the application_id of the managed identity
                application_id = self._azurerm.managed_identity_client_id(
                    managed_identity.access_connector_id,
                    managed_identity.managed_identity_id,
                )
            if not application_id:
                continue

            if read_only:
                app_id_mapping_read[application_id] = name
                continue
            app_id_mapping_write[application_id] = name

        return app_id_mapping_write, app_id_mapping_read

    def _prefix_credential_name_mapping(self) -> tuple[dict[str, str], dict[str, str]]:
        # get managed identity/service principal's application id to storage credential name mapping
        # for all non read-only and read-only credentials
        app_id_mapping_write, app_id_mapping_read = self._app_id_credential_name_mapping()

        # use the application id to storage credential name mapping to create prefix to storage credential name mapping
        prefix_mapping_write = {}
        prefix_mapping_read = {}
        for permission_mapping in self._resource_permissions.load():
            if permission_mapping.client_id in app_id_mapping_write:
                prefix_mapping_write[permission_mapping.prefix] = app_id_mapping_write[permission_mapping.client_id]
                continue
            if permission_mapping.client_id in app_id_mapping_read:
                prefix_mapping_read[permission_mapping.prefix] = app_id_mapping_read[permission_mapping.client_id]

        all_storage_accounts = list(self._azurerm.storage_accounts())
        for storage_credential in self._ws.storage_credentials.list():
            # Filter storage credentials for access connectors created by UCX
            if not (
                storage_credential.name is not None
                and storage_credential.name.startswith("ac-")
                and storage_credential.comment is not None
                and storage_credential.comment == "Created by UCX"
            ):
                continue

            storage_account_name = storage_credential.name.removeprefix("ac-")
            storage_accounts = [st for st in all_storage_accounts if st.name == storage_account_name]
            if len(storage_accounts) == 0:
                logger.warning(
                    f"Storage account {storage_account_name} for access connector {storage_credential.name} not found, "
                    "therefore, not able to create external locations for this storage account using the access "
                    "connector."
                )
                continue

            for container in self._azurerm.containers(storage_accounts[0].id):
                storage_url = f"abfss://{container.container}@{container.storage_account}.dfs.core.windows.net/"
                # UCX assigns created access connectors the "STORAGE_BLOB_DATA_CONTRIBUTOR" role on the storage account
                prefix_mapping_write[storage_url] = storage_credential.name

        return prefix_mapping_write, prefix_mapping_read

    def _create_location_name(self, location_url: str) -> str:
        # generate the UC external location name
        before_at, _, after_at = location_url.partition('@')
        container_name = before_at.removeprefix("abfss://")
        res_name = after_at.replace(".dfs.core.windows.net", "").rstrip("/").replace("/", "_")
        return f"{container_name}_{res_name}"

    def _create_external_location_helper(
        self, name, url, credential, comment="Created by UCX", read_only=False, skip_validation=False
    ) -> str | None:
        try:
            self._ws.external_locations.create(
                name, url, credential, comment=comment, read_only=read_only, skip_validation=skip_validation
            )
            return url
        except InvalidParameterValue as invalid:
            if "overlaps with an existing external location" in str(invalid):
                logger.warning(f"Skip creating external location, see details: {str(invalid)}")
                return None
            raise invalid

    def _create_external_location(
        self, location_url: str, prefix_mapping_write: dict[str, str], prefix_mapping_read: dict[str, str]
    ) -> str | None:
        location_name = self._create_location_name(location_url)

        # get container url as the prefix
        parsed_url = urlparse(location_url)
        container_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"

        # try to create external location with write privilege first
        if container_url in prefix_mapping_write:
            url = self._create_external_location_helper(
                location_name, location_url, prefix_mapping_write[container_url], comment="Created by UCX"
            )
            return url
        # if no matched write privilege credential, try to create read-only external location
        if container_url in prefix_mapping_read:
            try:
                url = self._create_external_location_helper(
                    location_name,
                    location_url,
                    prefix_mapping_read[container_url],
                    comment="Created by UCX",
                    read_only=True,
                )
                return url
            except PermissionDenied as denied:
                if "No file available under the location to read" in str(denied):
                    # Empty location will cause failed READ permission check with read-only credential
                    # Skip skip_validation in this case
                    url = self._create_external_location_helper(
                        location_name,
                        location_url,
                        prefix_mapping_read[container_url],
                        comment="Created by UCX",
                        read_only=True,
                        skip_validation=True,
                    )
                    return url
                raise denied
        # if no credential found
        return None

    def _filter_unsupported_location(self, location_urls: list[str]) -> list[str]:
        # remove unsupported external location
        supported_urls = []
        for url in location_urls:
            if url.startswith("abfss://"):
                supported_urls.append(url)
                continue
            logger.warning(f"Skip unsupported location: {url}")
        return supported_urls

    def run(self) -> list[str]:
        # list missing external locations in UC
        _, missing_locations = self._hms_locations.match_table_external_locations()
        # Extract the location URLs from the missing locations
        missing_loc_urls = [loc.location for loc in missing_locations]
        missing_loc_urls = self._filter_unsupported_location(missing_loc_urls)

        # get prefix to storage credential name mapping
        prefix_mapping_write, prefix_mapping_read = self._prefix_credential_name_mapping()

        # if missing external location is in prefix to storage credential name mapping
        # create a UC external location with mapped storage credential name
        migrated_loc_urls = []
        for location_url in missing_loc_urls:
            migrated_loc_url = self._create_external_location(location_url, prefix_mapping_write, prefix_mapping_read)
            if migrated_loc_url:
                migrated_loc_urls.append(migrated_loc_url)

        leftover_loc_urls = [url for url in missing_loc_urls if url not in migrated_loc_urls]
        self._principal_acl.apply_location_acl()
        if leftover_loc_urls:
            logger.info(
                "External locations below are not created in UC. You may check following cases and rerun this command:"
                "1. Please check the output of 'migrate_credentials' command for storage credentials migration failure."
                "2. If you use service principal in extra_config when create dbfs mount or use service principal "
                "in your code directly for storage access, UCX cannot automatically migrate them to storage credential."
                "Please manually create those storage credentials first."
                "3. You may have overlapping external location already in UC."
            )
            for loc_url in leftover_loc_urls:
                logger.info(f"Not created external location: {loc_url}")
            return leftover_loc_urls

        logger.info("All UC external location are created.")
        return leftover_loc_urls
