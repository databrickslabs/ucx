import base64
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterSource, Policy

from databricks.labs.ucx.assessment.crawlers import (
    _CLIENT_ENDPOINT_LENGTH,
    _SECRET_LIST_LENGTH,
    _SECRET_PATTERN,
    _STORAGE_ACCOUNT_EXTRACT_PATTERN,
    azure_sp_conf_present_check,
    logger,
)
from databricks.labs.ucx.assessment.jobs import JobsMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend


@dataclass
class AzureServicePrincipalInfo:
    # fs.azure.account.oauth2.client.id
    application_id: str | None
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_scope: str | None
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_key: str | None
    # fs.azure.account.oauth2.client.endpoint: "https://login.microsoftonline.com/${local.tenant_id}/oauth2/token"
    tenant_id: str | None
    # Azure Storage account to which the SP has been given access
    storage_account: str | None


def generate_service_principals(service_principals: list[dict]):
    for spn in service_principals:
        spn_info = AzureServicePrincipalInfo(
            application_id=spn.get("application_id"),
            secret_scope=spn.get("secret_scope"),
            secret_key=spn.get("secret_key"),
            tenant_id=spn.get("tenant_id"),
            storage_account=spn.get("storage_account"),
        )
        yield spn_info


class AzureServicePrincipalCrawler(CrawlerBase[AzureServicePrincipalInfo], JobsMixin):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "azure_service_principals", AzureServicePrincipalInfo)
        self._ws = ws

    def snapshot(self) -> Iterable[AzureServicePrincipalInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[AzureServicePrincipalInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AzureServicePrincipalInfo(*row)

    def _crawl(self) -> Iterable[AzureServicePrincipalInfo]:
        all_relevant_service_principals = self._get_relevant_service_principals()
        deduped_service_principals = [dict(t) for t in {tuple(d.items()) for d in all_relevant_service_principals}]
        return list(generate_service_principals(deduped_service_principals))

    def _get_relevant_service_principals(self) -> list:
        relevant_service_principals = []

        # list all relevant service principals in clusters
        for cluster_config in self._ws.clusters.list():
            if cluster_config.cluster_source != ClusterSource.JOB:
                relevant_service_principals.extend(self._get_azure_spn_from_cluster_config(cluster_config))

        # list all relevant service principals in pipelines
        for pipeline in self._ws.pipelines.list_pipelines():
            assert pipeline.pipeline_id is not None
            pipeline_info = self._ws.pipelines.get(pipeline.pipeline_id)
            assert pipeline_info.spec is not None
            pipeline_config = pipeline_info.spec.configuration
            if pipeline_config:
                if not azure_sp_conf_present_check(pipeline_config):
                    continue
                relevant_service_principals.extend(self._get_azure_spn_from_config(pipeline_config))

        # list all relevant service principals in jobs
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters_by_id = {c.cluster_id: c for c in self._ws.clusters.list()}
        for _job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            relevant_service_principals.extend(self._get_azure_spn_from_cluster_config(cluster_config))

        # list all relevant service principals in sql spark conf
        relevant_service_principals.extend(self._list_all_spn_in_sql_warehouses_spark_conf())
        return relevant_service_principals

    def _list_all_spn_in_sql_warehouses_spark_conf(self) -> list:
        warehouse_config_list = self._ws.warehouses.get_workspace_warehouse_config().data_access_config
        if warehouse_config_list is None or len(warehouse_config_list) == 0:
            return []
        warehouse_config_dict = {config.key: config.value for config in warehouse_config_list}
        if not azure_sp_conf_present_check(warehouse_config_dict):
            return []
        return self._get_azure_spn_from_config(warehouse_config_dict)

    def _get_azure_spn_from_cluster_config(self, cluster_config):
        azure_spn_list = []

        if cluster_config.spark_conf is not None:
            if azure_sp_conf_present_check(cluster_config.spark_conf):
                azure_spn_list.extend(self._get_azure_spn_from_config(cluster_config.spark_conf))

        if cluster_config.policy_id is None:
            return azure_spn_list

        policy = self._safe_get_cluster_policy(cluster_config.policy_id)

        if policy is None:
            return azure_spn_list

        if policy.definition is not None:
            if azure_sp_conf_present_check(json.loads(policy.definition)):
                azure_spn_list.extend(self._get_azure_spn_from_config(json.loads(policy.definition)))

        if policy.policy_family_definition_overrides is None:
            return azure_spn_list
        if azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
            azure_spn_list.extend(
                self._get_azure_spn_from_config(json.loads(policy.policy_family_definition_overrides))
            )

        return azure_spn_list

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def _get_azure_spn_from_config(self, config: dict) -> list:
        spn_list = []
        spn_application_id, secret_scope, secret_key, tenant_id, storage_account = None, "", "", "", ""
        matching_key_list = [key for key in config.keys() if "fs.azure.account.oauth2.client.id" in key]
        if len(matching_key_list) > 0:
            for key in matching_key_list:
                # retrieve application id of spn
                spn_application_id, secret_scope, secret_key = self._get_key_from_config(key, config)
                if spn_application_id is None:
                    continue

                # retrieve storage account configured with this spn
                storage_account_match = re.search(_STORAGE_ACCOUNT_EXTRACT_PATTERN, key)
                if storage_account_match:
                    storage_account = storage_account_match.group(1).strip(".")
                    tenant_key = "fs.azure.account.oauth2.client.endpoint." + storage_account
                else:
                    tenant_key = "fs.azure.account.oauth2.client.endpoint"

                # retrieve tenant id of spn
                matching_key = [key for key in config.keys() if re.search(tenant_key, key)]
                if len(matching_key) == 0 or matching_key[0] is None:
                    tenant_id = ""
                else:
                    client_endpoint_list = self._get_key_from_config(matching_key[0], config)[0].split("/")
                    if len(client_endpoint_list) == _CLIENT_ENDPOINT_LENGTH:
                        tenant_id = client_endpoint_list[3]

                # add output to spn list
                spn_list.append(
                    {
                        "application_id": spn_application_id,
                        "secret_scope": secret_scope,
                        "secret_key": secret_key,
                        "tenant_id": tenant_id,
                        "storage_account": storage_account,
                    }
                )
        return spn_list

    def _get_key_from_config(self, key: str, config: dict) -> tuple[str, str, str]:
        if re.search("spark_conf", key):
            value = config.get(key, {}).get("value", "")
        else:
            value = config.get(key, "")
        # retrieve from secret scope if used
        secret_matched = re.search(_SECRET_PATTERN, value)
        if secret_matched is None:
            return value, "", ""
        secret_string = secret_matched.group(1).split("/")
        value = self._get_secret_if_exists(secret_string)
        secret_scope, secret_key = secret_string[1], secret_string[2]
        return value, secret_scope, secret_key

    def _get_secret_if_exists(self, secret_string) -> str | None:
        if len(secret_string) == _SECRET_LIST_LENGTH:
            secret_scope, secret_key = secret_string[1], secret_string[2]
            try:
                # Return the decoded secret value in string format
                secret = self._ws.secrets.get_secret(secret_scope, secret_key)
                assert secret.value is not None
                return base64.b64decode(secret.value).decode("utf-8")
            except NotFound:
                logger.warning(f'removed on the backend: {"/".join(secret_string)}')
                return None
        return None
