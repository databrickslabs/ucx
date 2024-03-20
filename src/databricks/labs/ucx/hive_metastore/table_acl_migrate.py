import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ExternalLocationInfo

from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalClusterMapping,
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)
from databricks.labs.ucx.azure.access import (
    AzureResourcePermissions,
    StoragePermissionMapping,
)
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.mapping import TableMapping

logger = logging.getLogger(__name__)


@dataclass
class IAMPrincipal:
    # user or group or spn
    principal_type: str
    # id of the principal
    principal_id: str


@dataclass
class ClusterPrincipalMapping:
    # cluster id
    cluster_id: str
    # list of all principals that has access to this cluster id
    principals: list[IAMPrincipal]


class TableACLMigrate:
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        installation: Installation,
        spn_crawler: AzureServicePrincipalCrawler,
        resource_permission: AzureResourcePermissions,
        table_mapping: TableMapping,
    ):
        self._backend = backend
        self._ws = ws
        self._spn_crawler = spn_crawler
        self._installation = installation
        self._resource_permission = resource_permission
        self._table_mapping = table_mapping

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, installation: Installation, prompts: Prompts):
        msg = (
            "This cmd will migrate acl for all interactive clusters to the related storage account tables, "
            "Please confirm "
        )
        if not prompts.confirm(msg):
            return None

        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        azure_client = AzureAPIClient(
            ws.config.arm_environment.resource_manager_endpoint,
            ws.config.arm_environment.service_management_endpoint,
        )
        graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
        azurerm = AzureResources(azure_client, graph_client)
        locations = ExternalLocations(ws, sql_backend, config.inventory_database)

        resource_permissions = AzureResourcePermissions(installation, ws, azurerm, locations)
        spn_crawler = AzureServicePrincipalCrawler(ws, sql_backend, config.inventory_database)
        table_mapping = TableMapping(installation, ws, sql_backend)

        return cls(ws, sql_backend, installation, spn_crawler, resource_permissions, table_mapping)

    def migrate_cluster_acl(self):
        spn_cluster_mapping = self._spn_crawler.get_cluster_to_storage_mapping()
        external_locations = self._get_principal_prefix(spn_cluster_mapping)

        if len(spn_cluster_mapping) == 0:
            logger.info("There are no interactive clusters configured with service principals")
            return
        for cluster_spn in spn_cluster_mapping:
            # user_mapping = self._get_cluster_principal_mapping(cluster_spn.cluster_id)
            logger.info(f"Applying UC ACL for cluster id {cluster_spn.cluster_id}")
            for spn in cluster_spn.spn_info:
                self._apply_uc_permission(external_locations)

    def _get_principal_prefix(
        self, spn_cluster_mapping: list[AzureServicePrincipalClusterMapping]
    ) -> list[StoragePermissionMapping]:
        set_service_principals = set[AzureServicePrincipalInfo]()
        for spn_mapping in spn_cluster_mapping:
            set_service_principals.update(spn_mapping.spn_info)
        external_locations = []
        permission_mappings = self._resource_permission.load()
        for spn in set_service_principals:
            for perm_mapping in permission_mappings:
                if perm_mapping.client_id == spn.application_id and spn.storage_account in perm_mapping.prefix:
                    external_locations.append(perm_mapping)
        return external_locations

    def _get_tables(self, locations: list[ExternalLocationInfo]):
        matching_tables = []
        tables = self._table_mapping.load()
        for table in tables:
            table_name = f"{table.catalog_name}.{table.dst_schema}.{table.dst_table}"
            table_info = self._ws.tables.get(table_name)
            for location in locations:
                assert location.url is not None
                if table_info.storage_location is not None and table_info.storage_location.startswith(location.url):
                    matching_tables.append(location)
        return matching_tables

    def _get_external_location(self, locations: list[StoragePermissionMapping]) -> list[ExternalLocationInfo]:
        matching_location = []
        for location in self._ws.external_locations.list():
            for principal_prefix in locations:
                assert location.url is not None
                if location.url.startswith(principal_prefix.prefix):
                    matching_location.append(location)
        return matching_location

    def _apply_uc_permission(
        self,
        locations: list[StoragePermissionMapping],
    ):
        matching_external_locations = self._get_external_location(locations)
        matching_tables = self._get_tables(matching_external_locations)
        print(matching_tables)

    def _get_cluster_principal_mapping(self, cluster_id: str) -> list[IAMPrincipal]:
        # gets all the users,groups,spn which have access to the clusters and returns a dataclass of that mapping
        principal_list = []
        cluster_permission = self._ws.permissions.get("clusters", cluster_id)
        if cluster_permission.access_control_list is None:
            return []
        for acl in cluster_permission.access_control_list:
            if acl.user_name is not None:
                principal_list.append(IAMPrincipal("user", acl.user_name))
            if acl.group_name is not None:
                principal_list.append(IAMPrincipal("group", acl.group_name))
            if acl.service_principal_name is not None:
                principal_list.append(IAMPrincipal("spn", acl.service_principal_name))
        return principal_list

    def _get_spn_permission(self, spn_cluster_mapping: list[AzureServicePrincipalClusterMapping]):
        set_service_principals = set[AzureServicePrincipalInfo]()
        for spn in spn_cluster_mapping:
            set_service_principals.update(spn.spn_info)
        cluster_user_mapping = []
        for cluster in spn_cluster_mapping:
            principal_list = []
            cluster_permission = self._ws.permissions.get("clusters", cluster.cluster_id)
            if cluster_permission.access_control_list is None:
                return []
            for acl in cluster_permission.access_control_list:
                if acl.user_name is not None:
                    principal_list.append(IAMPrincipal("user", acl.user_name))
                if acl.group_name is not None:
                    principal_list.append(IAMPrincipal("group", acl.group_name))
                if acl.service_principal_name is not None:
                    principal_list.append(IAMPrincipal("spn", acl.service_principal_name))
            cluster_user_mapping.append(ClusterPrincipalMapping(cluster.cluster_id, principal_list))
        return cluster_user_mapping
