import json
import re
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterSource, DataSecurityMode, Policy

from databricks.labs.ucx.assessment.crawlers import azure_sp_conf_present_check, logger
from databricks.labs.ucx.assessment.jobs import JobsMixin
from databricks.labs.ucx.assessment.secrets import SecretsMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

_STORAGE_ACCOUNT_PATTERN = r"(?:id|endpoint)(.*?)dfs"


@dataclass(frozen=True)
class AzureServicePrincipalInfo:
    # fs.azure.account.oauth2.client.id
    application_id: str
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_scope: str | None = None
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_key: str | None = None
    # fs.azure.account.oauth2.client.endpoint: https://login.microsoftonline.com/${local.tenant_id}/oauth2/token
    tenant_id: str | None = None
    # Azure Storage account to which the SP has been given access
    storage_account: str | None = None


@dataclass
class ServicePrincipalClusterMapping:
    # this class is created separately as we need cluster to spn mapping
    # Cluster id where the spn is used
    cluster_id: str
    # spn info data class
    spn_info: set[AzureServicePrincipalInfo]


class AzureServicePrincipalCrawler(CrawlerBase[AzureServicePrincipalInfo], JobsMixin, SecretsMixin):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "azure_service_principals", AzureServicePrincipalInfo)
        self._ws = ws

    def _try_fetch(self) -> Iterable[AzureServicePrincipalInfo]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield AzureServicePrincipalInfo(*row)

    def _crawl(self) -> Iterable[AzureServicePrincipalInfo]:
        return self._get_relevant_service_principals()

    def _get_relevant_service_principals(self) -> list[AzureServicePrincipalInfo]:
        set_service_principals = set[AzureServicePrincipalInfo]()

        # list all relevant service principals in clusters
        for cluster_config in self._ws.clusters.list():
            if cluster_config.cluster_source != ClusterSource.JOB:
                set_service_principals.update(self._get_azure_spn_from_cluster_config(cluster_config))

        # list all relevant service principals in pipelines
        for pipeline in self._ws.pipelines.list_pipelines():
            assert pipeline.pipeline_id is not None
            pipeline_info = self._ws.pipelines.get(pipeline.pipeline_id)
            assert pipeline_info.spec is not None
            pipeline_config = pipeline_info.spec.configuration
            if pipeline_config:
                if not azure_sp_conf_present_check(pipeline_config):
                    continue
                set_service_principals.update(self._get_azure_spn_from_config(pipeline_config))

        # list all relevant service principals in jobs
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters_by_id = {c.cluster_id: c for c in self._ws.clusters.list()}
        for _job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            set_service_principals.update(self._get_azure_spn_from_cluster_config(cluster_config))

        # list all relevant service principals in sql spark conf
        set_service_principals.update(self._list_all_spn_in_sql_warehouses_spark_conf())
        return list(set_service_principals)

    def _list_all_spn_in_sql_warehouses_spark_conf(self) -> set[AzureServicePrincipalInfo]:
        warehouse_config_list = self._ws.warehouses.get_workspace_warehouse_config().data_access_config
        if warehouse_config_list is None or len(warehouse_config_list) == 0:
            return set[AzureServicePrincipalInfo]()
        warehouse_config_dict = {config.key: config.value for config in warehouse_config_list}
        if not azure_sp_conf_present_check(warehouse_config_dict):
            return set[AzureServicePrincipalInfo]()
        return self._get_azure_spn_from_config(warehouse_config_dict)

    def _get_azure_spn_from_cluster_config(self, cluster_config) -> set[AzureServicePrincipalInfo]:
        """Detect azure service principals from a Databricks cluster config
        This checks the Spark conf of the cluster config, and any spark conf in the cluster policy
        As well as cluster policy family override
        """
        set_service_principals = set[AzureServicePrincipalInfo]()

        if cluster_config.spark_conf is not None:
            if azure_sp_conf_present_check(cluster_config.spark_conf):
                set_service_principals.update(self._get_azure_spn_from_config(cluster_config.spark_conf))

        if cluster_config.policy_id is None:
            return set_service_principals

        policy = self._safe_get_cluster_policy(cluster_config.policy_id)

        if policy is None:
            return set_service_principals

        if policy.definition is not None:
            if azure_sp_conf_present_check(json.loads(policy.definition)):
                set_service_principals.update(self._get_azure_spn_from_config(json.loads(policy.definition)))

        if policy.policy_family_definition_overrides is None:
            return set_service_principals
        if azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
            set_service_principals.update(
                self._get_azure_spn_from_config(json.loads(policy.policy_family_definition_overrides))
            )

        return set_service_principals

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def _get_azure_spn_from_config(self, config: dict) -> set[AzureServicePrincipalInfo]:
        """Detect azure service principals from a dictionary of spark configs
        This checks for the existence of an application id by matching fs.azure.account.oauth2.client.id
        For each application id identified, retrieve the relevant storage account, client secret and tenant id
        """
        set_service_principals = set[AzureServicePrincipalInfo]()
        matching_spn_app_id_keys = [key for key in config.keys() if "fs.azure.account.oauth2.client.id" in key]
        for spn_app_id_key in matching_spn_app_id_keys:
            # retrieve application id of spn
            spn_application_id = self._get_value_from_config_key(config, spn_app_id_key)
            if spn_application_id is None:
                continue

            storage_account, secret_scope, secret_key, tenant_id = None, None, None, None
            tenant_key = "fs.azure.account.oauth2.client.endpoint"
            client_secret_key = "fs.azure.account.oauth2.client.secret"

            # retrieve storage account configured with this spn
            storage_account_matched = re.search(_STORAGE_ACCOUNT_PATTERN, spn_app_id_key)
            if storage_account_matched:
                storage_account = storage_account_matched.group(1).strip(".")
                # adjust the key to lookup for tenant id & secret id
                tenant_key = f"{tenant_key}.{storage_account}"
                client_secret_key = f"{client_secret_key}.{storage_account}"

            # retrieve client secret of spn
            matching_secret_keys = [key for key in config.keys() if client_secret_key in key]
            if len(matching_secret_keys) == 0 or matching_secret_keys[0] is None:
                secret_scope, secret_key = None, None
            else:
                secret_scope, secret_key = self._get_client_secret(config, matching_secret_keys[0])
            # retrieve tenant id of spn
            matching_tenant_keys = [key for key in config.keys() if tenant_key in key]
            if len(matching_tenant_keys) == 0 or matching_tenant_keys[0] is None:
                tenant_id = None
            else:
                tenant_id = self._get_tenant_id(config, matching_tenant_keys[0])

            # add output to the set - this automatically dedupe
            set_service_principals.add(
                AzureServicePrincipalInfo(
                    application_id=spn_application_id,
                    secret_scope=secret_scope,
                    secret_key=secret_key,
                    tenant_id=tenant_id,
                    storage_account=storage_account,
                )
            )
        return set_service_principals

    def get_cluster_to_storage_mapping(self) -> list[ServicePrincipalClusterMapping]:
        # this function gives a mapping between an interactive cluster and the spn used by it
        # either directly or through a cluster policy.
        set_service_principals = set[AzureServicePrincipalInfo]()
        spn_cluster_mapping = []
        for cluster in self._ws.clusters.list():
            if cluster.cluster_source != ClusterSource.JOB and (
                cluster.data_security_mode in [DataSecurityMode.LEGACY_SINGLE_USER, DataSecurityMode.NONE]
                and cluster.cluster_id is not None
            ):
                set_service_principals = self._get_azure_spn_from_cluster_config(cluster)
                spn_cluster_mapping.append(ServicePrincipalClusterMapping(cluster.cluster_id, set_service_principals))
        return spn_cluster_mapping

    def get_warehouse_to_storage_mapping(self) -> list[ServicePrincipalClusterMapping]:
        # this function gives a mapping between a sql warehouse and the spn used by it
        spn_warehouse_mapping = []
        set_service_principals = self._list_all_spn_in_sql_warehouses_spark_conf()
        if len(set_service_principals) == 0:
            return []
        for warehouse in self._ws.warehouses.list():
            if warehouse.id is not None:
                spn_warehouse_mapping.append(ServicePrincipalClusterMapping(warehouse.id, set_service_principals))
        return spn_warehouse_mapping
