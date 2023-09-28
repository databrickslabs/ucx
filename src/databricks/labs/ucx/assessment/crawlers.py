import json
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSource
from databricks.sdk.service.jobs import BaseJob

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

INCOMPATIBLE_SPARK_CONFIG_KEYS = [
    "spark.databricks.passthrough.enabled",
    "spark.hadoop.javax.jdo.option.ConnectionURL",
    "spark.databricks.hive.metastore.glueCatalog.enabled",
]

_AZURE_SP_CONF = [
    "fs.azure.account.key",
    "fs.azure.account.auth.type",
    "fs.azure.account.oauth.provider.type",
    "fs.azure.account.oauth2.client.id",
    "fs.azure.account.oauth2.client.secret",
    "fs.azure.account.oauth2.client.endpoint",
]

_AZURE_SP_CONF_FAILURE_MSG = "Uses azure service principal credentials config in "


@dataclass
class JobInfo:
    job_id: str
    job_name: str
    creator: str
    success: int
    failures: str


@dataclass
class ClusterInfo:
    cluster_id: str
    cluster_name: str
    creator: str
    success: int
    failures: str


@dataclass
class PipelineInfo:
    pipeline_id: str
    pipeline_name: str
    creator_name: str
    success: int
    failures: str


@dataclass
class AzureServicePrincipalInfo:
    active: bool
    application_id: str
    display_name: str
    entitlements: list
    external_id: str
    groups: list
    spn_id: str
    roles: list


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


class AzureServicePrincipalCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "azure_service_principals")
        self._ws = ws

    def _crawl(self) -> list[AzureServicePrincipalInfo]:
        all_service_principals = list(self._ws.service_principals.list())
        return list(self._assess_service_principals(all_service_principals))

    def _assess_service_principals(self, all_service_principals):
        _entitlements = []
        _groups = []
        _roles = []
        for spn in all_service_principals:
            if spn.entitlements:
                _entitlements = [entitlement.as_dict() for entitlement in spn.entitlements]
            if spn.groups:
                _groups = [group.as_dict() for group in spn.groups]
            if spn.roles:
                _roles = [role.as_dict() for role in spn.roles]
            spn_info = AzureServicePrincipalInfo(
                spn.active,
                spn.application_id,
                spn.display_name,
                _entitlements,
                spn.external_id,
                _groups,
                spn.id,
                _roles,
            )
            yield spn_info

    def snapshot(self) -> list[AzureServicePrincipalInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[AzureServicePrincipalInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AzureServicePrincipalInfo(*row)


class PipelinesCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "pipelines")
        self._ws = ws

    def _crawl(self) -> list[PipelineInfo]:
        all_pipelines = list(self._ws.pipelines.list_pipelines())
        return list(self._assess_pipelines(all_pipelines))

    def _assess_pipelines(self, all_pipelines):
        for pipeline in all_pipelines:
            pipeline_info = PipelineInfo(pipeline.pipeline_id, pipeline.name, pipeline.creator_user_name, 1, "")
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
        super().__init__(sbe, "hive_metastore", schema, "clusters")
        self._ws = ws

    def _crawl(self) -> list[ClusterInfo]:
        all_clusters = list(self._ws.clusters.list())
        return list(self._assess_clusters(all_clusters))

    def _assess_clusters(self, all_clusters):
        for cluster in all_clusters:
            if cluster.cluster_source == ClusterSource.JOB:
                continue
            cluster_info = ClusterInfo(cluster.cluster_id, cluster.cluster_name, cluster.creator_user_name, 1, "")
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
                policy = self._ws.cluster_policies.get(cluster.policy_id)
                if _azure_sp_conf_present_check(json.loads(policy.definition)):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")
                if policy.policy_family_definition_overrides:
                    if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
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
        super().__init__(sbe, "hive_metastore", schema, "jobs")
        self._ws = ws

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

    def _crawl(self) -> list[JobInfo]:
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_jobs(all_jobs, all_clusters)

    def _assess_jobs(self, all_jobs: list[BaseJob], all_clusters_by_id) -> list[JobInfo]:
        job_assessment = {}
        job_details = {}
        for job in all_jobs:
            job_assessment[job.job_id] = set()
            job_details[job.job_id] = JobInfo(str(job.job_id), job.settings.name, job.creator_user_name, 1, "")

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
                policy = self._ws.cluster_policies.get(cluster_config.policy_id)
                if _azure_sp_conf_present_check(json.loads(policy.definition)):
                    job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")
                if policy.policy_family_definition_overrides:
                    if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                        job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

        for job_key in job_details.keys():
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    def snapshot(self) -> list[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield JobInfo(*row)
