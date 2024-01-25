import json
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import compute
from databricks.sdk.service.compute import ClusterDetails, ClusterSource, Policy

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
class ClusterInfo:
    cluster_id: str
    success: int
    failures: str
    cluster_name: str | None = None
    creator: str | None = None


class ClustersMixin:
    _ws: WorkspaceClient

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def _check_spark_conf(self, cluster, failures):
        for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
            if k in cluster.spark_conf:
                failures.append(f"unsupported config: {k}")
        for value in cluster.spark_conf.values():
            if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                failures.append(f"using DBFS mount in configuration: {value}")
        # Checking if Azure cluster config is present in spark config
        if _azure_sp_conf_present_check(cluster.spark_conf):
            failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")

    def _check_cluster_policy(self, cluster, failures):
        policy = self._safe_get_cluster_policy(cluster.policy_id)
        if policy:
            if policy.definition:
                if _azure_sp_conf_present_check(json.loads(policy.definition)):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")
            if policy.policy_family_definition_overrides:
                if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")

    def _check_init_scripts(self, cluster, failures):
        for init_script_info in cluster.init_scripts:
            init_script_data = _get_init_script_data(self._ws, init_script_info)
            if not init_script_data:
                continue
            if not _azure_sp_conf_in_init_scripts(init_script_data):
                continue
            failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")

    def _check_cluster_failures(self, cluster: ClusterDetails | compute.ClusterSpec):
        failures = []
        if isinstance(cluster, ClusterDetails) and not cluster.creator_user_name:
            logger.warning(
                f"Cluster {cluster.cluster_id} have Unknown creator, it means that the original creator "
                f"has been deleted and should be re-created"
            )
        cluster_id = cluster.cluster_id if isinstance(cluster, ClusterDetails) and cluster.cluster_id else ""
        creator_user_name = (
            cluster.creator_user_name if isinstance(cluster, ClusterDetails) and cluster.creator_user_name else None
        )
        cluster_info = ClusterInfo(
            cluster_id=cluster_id,
            cluster_name=cluster.cluster_name,
            creator=creator_user_name,
            success=1,
            failures="[]",
        )
        support_status = spark_version_compatibility(cluster.spark_version)
        if support_status != "supported":
            failures.append(f"not supported DBR: {cluster.spark_version}")
        if cluster.spark_conf is not None:
            self._check_spark_conf(cluster, failures)
        # Checking if Azure cluster config is present in cluster policies
        if cluster.policy_id:
            self._check_cluster_policy(cluster, failures)
        if cluster.init_scripts:
            self._check_init_scripts(cluster, failures)
        cluster_info.failures = json.dumps(failures)
        if len(failures) > 0:
            cluster_info.success = 0
        return cluster_info


class ClustersCrawler(CrawlerBase[ClusterInfo], ClustersMixin):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "clusters", ClusterInfo)
        self._ws = ws

    def _crawl(self) -> Iterable[ClusterInfo]:
        all_clusters = list(self._ws.clusters.list())
        return list(self._assess_clusters(all_clusters))

    def _assess_clusters(self, all_clusters):
        for cluster in all_clusters:
            if cluster.cluster_source == ClusterSource.JOB:
                continue
            yield self._check_cluster_failures(cluster)

    def snapshot(self) -> Iterable[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ClusterInfo(*row)
