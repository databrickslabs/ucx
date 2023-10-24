import base64
import json
import logging
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.compute import ClusterSource
from databricks.sdk.service.jobs import BaseJob

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

logger = logging.getLogger(__name__)

INCOMPATIBLE_SPARK_CONFIG_KEYS = [
    "spark.databricks.passthrough.enabled",
    "spark.hadoop.javax.jdo.option.ConnectionURL",
    "spark.databricks.hive.metastore.glueCatalog.enabled",
]

_AZURE_SP_CONF = [
    "fs.azure.account.auth.type",
    "fs.azure.account.oauth.provider.type",
    "fs.azure.account.oauth2.client.id",
    "fs.azure.account.oauth2.client.secret",
    "fs.azure.account.oauth2.client.endpoint",
]
_SECRET_PATTERN = r"{{(secrets.*?)}}"
_STORAGE_ACCOUNT_EXTRACT_PATTERN = r"(?:id|endpoint)(.*?)dfs"
_AZURE_SP_CONF_FAILURE_MSG = "Uses azure service principal credentials config in"
_SECRET_LIST_LENGTH = 3
_CLIENT_ENDPOINT_LENGTH = 6
_INIT_SCRIPT_DBFS_PATH = 2


@dataclass
class JobInfo:
    job_id: str
    job_name: str
    success: int
    failures: str
    creator: str = None


@dataclass
class ClusterInfo:
    cluster_id: str
    cluster_name: str
    success: int
    failures: str
    creator: str = None


@dataclass
class PipelineInfo:
    pipeline_id: str
    pipeline_name: str
    success: int
    failures: str
    creator_name: str = None


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


@dataclass
class GlobalInitScriptInfo:
    script_id: str
    script_name: str
    enabled: bool
    success: int
    failures: str
    created_by: str = None


def _get_init_script_data(w, init_script_info):
    if init_script_info.dbfs:
        if len(init_script_info.dbfs.destination.split(":")) == _INIT_SCRIPT_DBFS_PATH:
            file_api_format_destination = init_script_info.dbfs.destination.split(":")[1]
            if file_api_format_destination:
                try:
                    data = w.dbfs.read(file_api_format_destination).data
                    return base64.b64decode(data).decode("utf-8")
                except Exception:
                    return None
    if init_script_info.workspace:
        workspace_file_destination = init_script_info.workspace.destination
        if workspace_file_destination:
            try:
                data = w.workspace.export(workspace_file_destination).content
                return base64.b64decode(data).decode("utf-8")
            except Exception:
                return None


def _azure_sp_conf_in_init_scripts(init_script_data: str) -> bool:
    for conf in _AZURE_SP_CONF:
        if re.search(conf, init_script_data):
            return True
    return False


def _azure_sp_conf_present_check(config: dict) -> bool:
    for key in config.keys():
        for conf in _AZURE_SP_CONF:
            if re.search(conf, key):
                return True
    return False


def spark_version_compatibility(spark_version: str) -> str:
    first_comp_custom_rt = 3
    first_comp_custom_x = 2
    dbr_version_components = spark_version.split("-")
    first_components = dbr_version_components[0].split(".")
    if len(first_components) != first_comp_custom_rt:
        # custom runtime
        return "unsupported"
    if first_components[first_comp_custom_x] != "x":
        # custom runtime
        return "unsupported"
    version = int(first_components[0]), int(first_components[1])
    if version < (10, 0):
        return "unsupported"
    if (10, 0) <= version < (11, 3):
        return "kinda works"
    return "supported"


class GlobalInitScriptCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "global_init_scripts", GlobalInitScriptInfo)
        self._ws = ws

    def _crawl(self) -> list[GlobalInitScriptInfo]:
        all_global_init_scripts = list(self._ws.global_init_scripts.list())
        return list(self._assess_global_init_scripts(all_global_init_scripts))

    def _assess_global_init_scripts(self, all_global_init_scripts):
        for gis in all_global_init_scripts:
            if not gis.created_by:
                logger.warning(
                    f"Script {gis.name} have Unknown creator, it means that the original creator has been deleted"
                    f" and should be re-created"
                )
            global_init_script_info = GlobalInitScriptInfo(gis.script_id, gis.name, gis.enabled, 1, "", gis.created_by)
            failures = []
            global_init_script = base64.b64decode(self._ws.global_init_scripts.get(gis.script_id).script).decode(
                "utf-8"
            )
            if not global_init_script:
                continue
            if _azure_sp_conf_in_init_scripts(global_init_script):
                failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} global init script.")
                global_init_script_info.failures = json.dumps(failures)

            if len(failures) > 0:
                global_init_script_info.success = 0
            yield global_init_script_info

    def snapshot(self) -> list[GlobalInitScriptInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[GlobalInitScriptInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield GlobalInitScriptInfo(*row)


class AzureServicePrincipalCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "azure_service_principals", AzureServicePrincipalInfo)
        self._ws = ws

    def _crawl(self) -> list[AzureServicePrincipalInfo]:
        all_relevant_service_principals = self._get_relevant_service_principals()
        deduped_service_principals = [dict(t) for t in {tuple(d.items()) for d in all_relevant_service_principals}]
        return list(self._assess_service_principals(deduped_service_principals))

    def _check_secret_and_get_application_id(self, secret_matched) -> str:
        split = secret_matched.group(1).split("/")
        if len(split) == _SECRET_LIST_LENGTH:
            secret_scope, secret_key = split[1], split[2]
            try:
                # Return the decoded secret value in string format
                return base64.b64decode(self._ws.secrets.get_secret(secret_scope, secret_key).value).decode("utf-8")
            except DatabricksError as err:
                logger.warning(f"Error retrieving secret for {secret_matched.group(1)}. Error: {err}")

    def _get_azure_spn_tenant_id(self, config: dict, tenant_key: str) -> str:
        matching_key = [key for key in config.keys() if re.search(tenant_key, key)]
        if len(matching_key) > 0:
            if re.search("spark_conf", matching_key[0]):
                client_endpoint_list = config.get(matching_key[0]).get("value").split("/")
            else:
                client_endpoint_list = config.get(matching_key[0]).split("/")
            if len(client_endpoint_list) == _CLIENT_ENDPOINT_LENGTH:
                return client_endpoint_list[3]

    def _get_azure_spn_list(self, config: dict) -> list:
        spn_list = []
        secret_scope, secret_key, tenant_id, storage_account = None, None, None, None
        matching_key_list = [key for key in config.keys() if "fs.azure.account.oauth2.client.id" in key]
        if len(matching_key_list) > 0:
            for key in matching_key_list:
                storage_account_match = re.search(_STORAGE_ACCOUNT_EXTRACT_PATTERN, key)
                if re.search("spark_conf", key):
                    spn_application_id = config.get(key).get("value")
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
        azure_spn_list_with_data_access_from_jobs = []
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters_by_id = {c.cluster_id: c for c in self._ws.clusters.list()}
        for _job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            if cluster_config.spark_conf:
                if _azure_sp_conf_present_check(cluster_config.spark_conf):
                    temp_list = self._get_azure_spn_list(cluster_config.spark_conf)
                    if temp_list:
                        azure_spn_list_with_data_access_from_jobs += temp_list

            if cluster_config.policy_id:
                policy = self._ws.cluster_policies.get(cluster_config.policy_id)
                if policy.definition:
                    if _azure_sp_conf_present_check(json.loads(policy.definition)):
                        temp_list = self._get_azure_spn_list(json.loads(policy.definition))
                        if temp_list:
                            azure_spn_list_with_data_access_from_jobs += temp_list

                if policy.policy_family_definition_overrides:
                    if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                        temp_list = self._get_azure_spn_list(json.loads(policy.policy_family_definition_overrides))
                        if temp_list:
                            azure_spn_list_with_data_access_from_jobs += temp_list
        return azure_spn_list_with_data_access_from_jobs

    def _list_all_spn_in_sql_warehouses_spark_conf(self) -> list:
        warehouse_config_list = self._ws.warehouses.get_workspace_warehouse_config().data_access_config
        if warehouse_config_list:
            if len(warehouse_config_list) > 0:
                warehouse_config_dict = {config.key: config.value for config in warehouse_config_list}
                if warehouse_config_dict:
                    if _azure_sp_conf_present_check(warehouse_config_dict):
                        return self._get_azure_spn_list(warehouse_config_dict)

    def _list_all_pipeline_with_spn_in_spark_conf(self) -> list:
        azure_spn_list_with_data_access_from_pipeline = []
        for pipeline in self._ws.pipelines.list_pipelines():
            pipeline_config = self._ws.pipelines.get(pipeline.pipeline_id).spec.configuration
            if pipeline_config:
                if not _azure_sp_conf_present_check(pipeline_config):
                    continue
                temp_list = self._get_azure_spn_list(pipeline_config)
                if temp_list:
                    azure_spn_list_with_data_access_from_pipeline += temp_list
        return azure_spn_list_with_data_access_from_pipeline

    def _list_all_cluster_with_spn_in_spark_conf(self) -> list:
        azure_spn_list_with_data_access_from_cluster = []
        for cluster in self._ws.clusters.list():
            if cluster.cluster_source != ClusterSource.JOB:
                if cluster.spark_conf:
                    if _azure_sp_conf_present_check(cluster.spark_conf):
                        temp_list = self._get_azure_spn_list(cluster.spark_conf)
                        if temp_list:
                            azure_spn_list_with_data_access_from_cluster += temp_list

                if cluster.policy_id:
                    policy = self._ws.cluster_policies.get(cluster.policy_id)
                    if policy.definition:
                        if _azure_sp_conf_present_check(json.loads(policy.definition)):
                            temp_list = self._get_azure_spn_list(json.loads(policy.definition))
                            if temp_list:
                                azure_spn_list_with_data_access_from_cluster += temp_list

                    if policy.policy_family_definition_overrides:
                        if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                            temp_list = self._get_azure_spn_list(json.loads(policy.policy_family_definition_overrides))
                            if temp_list:
                                azure_spn_list_with_data_access_from_cluster += temp_list
        return azure_spn_list_with_data_access_from_cluster

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

    def snapshot(self) -> list[AzureServicePrincipalInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[AzureServicePrincipalInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AzureServicePrincipalInfo(*row)


class PipelinesCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "pipelines", PipelineInfo)
        self._ws = ws

    def _crawl(self) -> list[PipelineInfo]:
        all_pipelines = list(self._ws.pipelines.list_pipelines())
        return list(self._assess_pipelines(all_pipelines))

    def _assess_pipelines(self, all_pipelines):
        for pipeline in all_pipelines:
            if not pipeline.creator_user_name:
                logger.warning(
                    f"Pipeline {pipeline.name} have Unknown creator, it means that the original creator "
                    f"has been deleted and should be re-created"
                )
            pipeline_info = PipelineInfo(pipeline.pipeline_id, pipeline.name, 1, "", pipeline.creator_user_name)

            failures = []
            pipeline_config = self._ws.pipelines.get(pipeline.pipeline_id).spec.configuration
            if pipeline_config:
                if _azure_sp_conf_present_check(pipeline_config):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} pipeline.")

            pipeline_info.failures = json.dumps(failures)
            if len(failures) > 0:
                pipeline_info.success = 0
            yield pipeline_info

    def snapshot(self) -> list[PipelineInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[PipelineInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield PipelineInfo(*row)


class ClustersCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "clusters", ClusterInfo)
        self._ws = ws

    def _crawl(self) -> list[ClusterInfo]:
        all_clusters = list(self._ws.clusters.list())
        return list(self._assess_clusters(all_clusters))

    def _assess_clusters(self, all_clusters):
        for cluster in all_clusters:
            if cluster.cluster_source == ClusterSource.JOB:
                continue
            if not cluster.creator_user_name:
                logger.warning(
                    f"Cluster {cluster.cluster_id} have Unknown creator, it means that the original creator "
                    f"has been deleted and should be re-created"
                )
            cluster_info = ClusterInfo(
                cluster.cluster_id,
                cluster.cluster_name,
                1,
                "",
                cluster.creator_user_name,
            )
            support_status = spark_version_compatibility(cluster.spark_version)
            failures = []
            if support_status != "supported":
                failures.append(f"not supported DBR: {cluster.spark_version}")

            if cluster.spark_conf is not None:
                for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
                    if k in cluster.spark_conf:
                        failures.append(f"unsupported config: {k}")

                for value in cluster.spark_conf.values():
                    if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                        failures.append(f"using DBFS mount in configuration: {value}")

                # Checking if Azure cluster config is present in spark config
                if _azure_sp_conf_present_check(cluster.spark_conf):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")

            # Checking if Azure cluster config is present in cluster policies
            if cluster.policy_id:
                try:
                    policy = self._ws.cluster_policies.get(cluster.policy_id)
                    if policy.definition:
                        if _azure_sp_conf_present_check(json.loads(policy.definition)):
                            failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")
                    if policy.policy_family_definition_overrides:
                        if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                            failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")
                except DatabricksError as err:
                    logger.warning(f"Error retrieving cluster policy {cluster.policy_id}. Error: {err}")

            if cluster.init_scripts:
                for init_script_info in cluster.init_scripts:
                    init_script_data = _get_init_script_data(self._ws, init_script_info)
                    if not init_script_data:
                        continue
                    if not _azure_sp_conf_in_init_scripts(init_script_data):
                        continue
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")

            cluster_info.failures = json.dumps(failures)
            if len(failures) > 0:
                cluster_info.success = 0
            yield cluster_info

    def snapshot(self) -> list[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ClusterInfo(*row)


class JobsCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "jobs", JobInfo)
        self._ws = ws

    @staticmethod
    def _get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
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

    def _crawl(self) -> list[JobInfo]:
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_jobs(all_jobs, all_clusters)

    def _assess_jobs(self, all_jobs: list[BaseJob], all_clusters_by_id) -> list[JobInfo]:
        job_assessment = {}
        job_details = {}
        for job in all_jobs:
            job_assessment[job.job_id] = set()
            if not job.creator_user_name:
                logger.warning(
                    f"Job {job.job_id} have Unknown creator, it means that the original creator has been deleted "
                    f"and should be re-created"
                )
            job_details[job.job_id] = JobInfo(str(job.job_id), job.settings.name, 1, "", job.creator_user_name)

        for job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            support_status = spark_version_compatibility(cluster_config.spark_version)
            if support_status != "supported":
                job_assessment[job.job_id].add(f"not supported DBR: {cluster_config.spark_version}")

            if cluster_config.spark_conf is not None:
                for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
                    if k in cluster_config.spark_conf:
                        job_assessment[job.job_id].add(f"unsupported config: {k}")

                for value in cluster_config.spark_conf.values():
                    if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                        job_assessment[job.job_id].add(f"using DBFS mount in configuration: {value}")

                # Checking if Azure cluster config is present in spark config
                if _azure_sp_conf_present_check(cluster_config.spark_conf):
                    job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

            # Checking if Azure cluster config is present in cluster policies
            if cluster_config.policy_id:
                try:
                    policy = self._ws.cluster_policies.get(cluster_config.policy_id)
                    if policy.definition:
                        if _azure_sp_conf_present_check(json.loads(policy.definition)):
                            job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")
                    if policy.policy_family_definition_overrides:
                        if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                            job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")
                except DatabricksError as err:
                    logger.warning(f"Error retrieving cluster policy {cluster_config.policy_id}. Error: {err}")

            if cluster_config.init_scripts:
                for init_script_info in cluster_config.init_scripts:
                    init_script_data = _get_init_script_data(self._ws, init_script_info)
                    if not init_script_data:
                        continue
                    if not _azure_sp_conf_in_init_scripts(init_script_data):
                        continue
                    job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

        for job_key in job_details.keys():
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    def snapshot(self) -> list[JobInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[JobInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield JobInfo(*row)
