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
    _azure_sp_conf_present_check,
    logger,
)
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend


@dataclass
class AzureServicePrincipalInfo:
    # fs.azure.account.oauth2.client.id
    application_id: str
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_scope: str
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_key: str
    # fs.azure.account.oauth2.client.endpoint: "https://login.microsoftonline.com/${local.tenant_id}/oauth2/token"
    tenant_id: str
    # Azure Storage account to which the SP has been given access
    storage_account: str


class AzureServicePrincipalCrawler(CrawlerBase[AzureServicePrincipalInfo]):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "azure_service_principals", AzureServicePrincipalInfo)
        self._ws = ws

    def _crawl(self) -> Iterable[AzureServicePrincipalInfo]:
        all_relevant_service_principals = self._get_relevant_service_principals()
        deduped_service_principals = [dict(t) for t in {tuple(d.items()) for d in all_relevant_service_principals}]
        return list(self._assess_service_principals(deduped_service_principals))

    def _check_secret_and_get_application_id(self, secret_matched) -> str | None:
        split = secret_matched.group(1).split("/")
        if len(split) == _SECRET_LIST_LENGTH:
            secret_scope, secret_key = split[1], split[2]
            try:
                # Return the decoded secret value in string format
                secret = self._ws.secrets.get_secret(secret_scope, secret_key)
                assert secret.value is not None
                return base64.b64decode(secret.value).decode("utf-8")
            except NotFound:
                logger.warning(f'removed on the backend: {"/".join(split)}')
                return None
        return None

    def _get_azure_spn_tenant_id(self, config: dict, tenant_key: str) -> str | None:
        matching_key = [key for key in config.keys() if re.search(tenant_key, key)]
        if len(matching_key) > 0:
            matched = matching_key[0]
            if not matched:
                return None
            if re.search("spark_conf", matched):
                client_endpoint_list = config.get(matched, {}).get("value", "").split("/")
            else:
                client_endpoint_list = config.get(matched, "").split("/")
            if len(client_endpoint_list) == _CLIENT_ENDPOINT_LENGTH:
                return client_endpoint_list[3]
        return None

    def _get_azure_spn_list(self, config: dict) -> list:
        spn_list = []
        secret_scope, secret_key, tenant_id, storage_account = None, None, None, None
        matching_key_list = [key for key in config.keys() if "fs.azure.account.oauth2.client.id" in key]
        if len(matching_key_list) > 0:
            for key in matching_key_list:
                storage_account_match = re.search(_STORAGE_ACCOUNT_EXTRACT_PATTERN, key)
                if re.search("spark_conf", key):
                    spn_application_id = config.get(key, {}).get("value")
                else:
                    spn_application_id = config.get(key)
                if not spn_application_id:
                    continue
                secret_matched = re.search(_SECRET_PATTERN, spn_application_id)
                if secret_matched:
                    spn_application_id = self._check_secret_and_get_application_id(secret_matched)
                    if not spn_application_id:
                        continue
                    secret_scope, secret_key = (
                        secret_matched.group(1).split("/")[1],
                        secret_matched.group(1).split("/")[2],
                    )
                if storage_account_match:
                    storage_account = storage_account_match.group(1).strip(".")
                    tenant_key = "fs.azure.account.oauth2.client.endpoint." + storage_account
                else:
                    tenant_key = "fs.azure.account.oauth2.client.endpoint"
                tenant_id = self._get_azure_spn_tenant_id(config, tenant_key)

                spn_application_id = "" if spn_application_id is None else spn_application_id
                secret_scope = "" if secret_scope is None else secret_scope
                secret_key = "" if secret_key is None else secret_key
                tenant_id = "" if tenant_id is None else tenant_id
                storage_account = "" if storage_account is None else storage_account
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

    def _get_cluster_configs_from_all_jobs(self, all_jobs, all_clusters_by_id):
        for j in all_jobs:
            if j.settings.job_clusters is not None:
                for jc in j.settings.job_clusters:
                    if jc.new_cluster is None:
                        continue
                    yield j, jc.new_cluster

            for t in j.settings.tasks:
                if t.existing_cluster_id is not None:
                    interactive_cluster = all_clusters_by_id.get(t.existing_cluster_id, None)
                    if interactive_cluster is None:
                        continue
                    yield j, interactive_cluster

                elif t.new_cluster is not None:
                    yield j, t.new_cluster

    def _get_relevant_service_principals(self) -> list:
        relevant_service_principals = []
        temp_list = self._list_all_cluster_with_spn_in_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        temp_list = self._list_all_pipeline_with_spn_in_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        temp_list = self._list_all_jobs_with_spn_in_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        temp_list = self._list_all_spn_in_sql_warehouses_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        return relevant_service_principals

    def _list_all_jobs_with_spn_in_spark_conf(self) -> list:
        azure_spn_list = []
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters_by_id = {c.cluster_id: c for c in self._ws.clusters.list()}

        for _job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            azure_spn_list += self._get_azure_spn_with_data_access(cluster_config)

        return azure_spn_list

    def _list_all_cluster_with_spn_in_spark_conf(self) -> list:
        azure_spn_list = []

        for cluster_config in self._ws.clusters.list():
            if cluster_config.cluster_source != ClusterSource.JOB:
                azure_spn_list += self._get_azure_spn_with_data_access(cluster_config)

        return azure_spn_list

    def _get_azure_spn_with_data_access(self, cluster_config):
        azure_spn_list = []

        if cluster_config.spark_conf:
            if _azure_sp_conf_present_check(cluster_config.spark_conf):
                temp_list = self._get_azure_spn_list(cluster_config.spark_conf)
                if temp_list:
                    azure_spn_list += temp_list

        if cluster_config.policy_id:
            policy = self._safe_get_cluster_policy(cluster_config.policy_id)

            if policy is None:
                return azure_spn_list

            if policy.definition:
                if _azure_sp_conf_present_check(json.loads(policy.definition)):
                    temp_list = self._get_azure_spn_list(json.loads(policy.definition))
                    if temp_list:
                        azure_spn_list += temp_list

            if policy.policy_family_definition_overrides:
                if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                    temp_list = self._get_azure_spn_list(json.loads(policy.policy_family_definition_overrides))
                    if temp_list:
                        azure_spn_list += temp_list

        return azure_spn_list

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def _list_all_spn_in_sql_warehouses_spark_conf(self) -> list:
        warehouse_config_list = self._ws.warehouses.get_workspace_warehouse_config().data_access_config
        if warehouse_config_list:
            if len(warehouse_config_list) > 0:
                warehouse_config_dict = {config.key: config.value for config in warehouse_config_list}
                if warehouse_config_dict:
                    if _azure_sp_conf_present_check(warehouse_config_dict):
                        return self._get_azure_spn_list(warehouse_config_dict)
        return []

    def _list_all_pipeline_with_spn_in_spark_conf(self) -> list:
        azure_spn_list_with_data_access_from_pipeline = []
        for pipeline in self._ws.pipelines.list_pipelines():
            assert pipeline.pipeline_id is not None
            pipeline_info = self._ws.pipelines.get(pipeline.pipeline_id)
            assert pipeline_info.spec is not None
            pipeline_config = pipeline_info.spec.configuration
            if pipeline_config:
                if not _azure_sp_conf_present_check(pipeline_config):
                    continue
                temp_list = self._get_azure_spn_list(pipeline_config)
                if temp_list:
                    azure_spn_list_with_data_access_from_pipeline += temp_list
        return azure_spn_list_with_data_access_from_pipeline

    def _assess_service_principals(self, relevant_service_principals: list):
        for spn in relevant_service_principals:
            spn_info = AzureServicePrincipalInfo(
                application_id=spn.get("application_id"),
                secret_scope=spn.get("secret_scope"),
                secret_key=spn.get("secret_key"),
                tenant_id=spn.get("tenant_id"),
                storage_account=spn.get("storage_account"),
            )
            yield spn_info

    def snapshot(self) -> Iterable[AzureServicePrincipalInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[AzureServicePrincipalInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AzureServicePrincipalInfo(*row)
