import json
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterSource, Policy

from databricks.labs.ucx.assessment.crawlers import (
    _check_spark_conf,
    _check_cluster_policy,
    _check_init_scripts,
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


class ClustersCrawler(CrawlerBase[ClusterInfo]):
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
            if not cluster.creator_user_name:
                logger.warning(
                    f"Cluster {cluster.cluster_id} have Unknown creator, it means that the original creator "
                    f"has been deleted and should be re-created"
                )
            cluster_info = ClusterInfo(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name,
                creator=cluster.creator_user_name,
                success=1,
                failures="[]",
            )

            # check spark version
            support_status = spark_version_compatibility(cluster.spark_version)
            failures = []
            if support_status != "supported":
                failures.append(f"not supported DBR: {cluster.spark_version}")

            # check spark config
            if cluster.spark_conf is not None:
                failures.extend(_check_spark_conf(cluster.spark_conf, "cluster"))

            # Checking if Azure cluster config is present in cluster policies
            if cluster.policy_id:
                _check_cluster_policy(self._ws, cluster, failures)

            if cluster.init_scripts:
                _check_init_scripts(self._ws, cluster, failures)

            cluster_info.failures = json.dumps(failures)
            if len(failures) > 0:
                cluster_info.success = 0
            yield cluster_info

    def snapshot(self) -> Iterable[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ClusterInfo(*row)
