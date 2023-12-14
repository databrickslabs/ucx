import json
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import Policy
from databricks.sdk.service.jobs import BaseJob

from databricks.labs.ucx.assessment.crawlers import (
    _AZURE_SP_CONF_FAILURE_MSG,
    INCOMPATIBLE_SPARK_CONFIG_KEYS,
    _azure_sp_conf_in_init_scripts,
    _azure_sp_conf_present_check,
    _get_init_script_data,
    logger,
    spark_version_compatibility,
)
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend


@dataclass
class JobInfo:
    job_id: str
    success: int
    failures: str
    job_name: str | None = None
    creator: str | None = None


class JobsCrawler(CrawlerBase[JobInfo]):
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

    def _crawl(self) -> Iterable[JobInfo]:
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_jobs(all_jobs, all_clusters)

    def _assess_jobs(self, all_jobs: list[BaseJob], all_clusters_by_id) -> Iterable[JobInfo]:
        job_assessment: dict[int, set[str]] = {}
        job_details: dict[int, JobInfo] = {}
        for job in all_jobs:
            if not job.job_id:
                continue
            job_assessment[job.job_id] = set()
            if not job.creator_user_name:
                logger.warning(
                    f"Job {job.job_id} have Unknown creator, it means that the original creator has been deleted "
                    f"and should be re-created"
                )

            job_settings = job.settings
            assert job_settings is not None
            job_name = job_settings.name
            if not job_name:
                job_name = "Unknown"
            job_details[job.job_id] = JobInfo(
                job_id=str(job.job_id),
                job_name=job_name,
                creator=job.creator_user_name,
                success=1,
                failures="[]",
            )

        for job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            support_status = spark_version_compatibility(cluster_config.spark_version)
            job_id = job.job_id
            if not job_id:
                continue
            if support_status != "supported":
                job_assessment[job_id].add(f"not supported DBR: {cluster_config.spark_version}")

            if cluster_config.spark_conf is not None:
                for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
                    if k in cluster_config.spark_conf:
                        job_assessment[job_id].add(f"unsupported config: {k}")

                for value in cluster_config.spark_conf.values():
                    if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                        job_assessment[job_id].add(f"using DBFS mount in configuration: {value}")

                # Checking if Azure cluster config is present in spark config
                if _azure_sp_conf_present_check(cluster_config.spark_conf):
                    job_assessment[job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

            # Checking if Azure cluster config is present in cluster policies
            if cluster_config.policy_id:
                policy = self._safe_get_cluster_policy(cluster_config.policy_id)

                if policy is not None:
                    if policy.definition:
                        if _azure_sp_conf_present_check(json.loads(policy.definition)):
                            job_assessment[job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")
                    if policy.policy_family_definition_overrides:
                        if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                            job_assessment[job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

            if cluster_config.init_scripts:
                for init_script_info in cluster_config.init_scripts:
                    init_script_data = _get_init_script_data(self._ws, init_script_info)
                    if not init_script_data:
                        continue
                    if not _azure_sp_conf_in_init_scripts(init_script_data):
                        continue
                    job_assessment[job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

        for job_key in job_details.keys():
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def snapshot(self) -> Iterable[JobInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[JobInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield JobInfo(*row)
