import base64
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import (
    ClusterDetails,
    ClusterSource,
    DataSecurityMode,
    InitScriptInfo,
    Policy,
)

from databricks.labs.ucx.assessment.crawlers import (
    INIT_SCRIPT_DBFS_PATH,
    AZURE_SP_CONF_FAILURE_MSG,
    INCOMPATIBLE_SPARK_CONFIG_KEYS,
    azure_sp_conf_present_check,
    spark_version_compatibility,
)
from databricks.labs.ucx.assessment.init_scripts import CheckInitScriptMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

logger = logging.getLogger(__name__)


@dataclass
class ClusterInfo:
    cluster_id: str
    success: int
    failures: str
    cluster_name: str | None = None
    creator: str | None = None


class CheckClusterMixin(CheckInitScriptMixin):
    _ws: WorkspaceClient

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def _check_cluster_policy(self, policy_id: str, source: str) -> list[str]:
        failures: list[str] = []
        policy = self._safe_get_cluster_policy(policy_id)
        if policy:
            if policy.definition:
                if azure_sp_conf_present_check(json.loads(policy.definition)):
                    failures.append(f"{AZURE_SP_CONF_FAILURE_MSG} {source}.")
            if policy.policy_family_definition_overrides:
                if azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                    failures.append(f"{AZURE_SP_CONF_FAILURE_MSG} {source}.")
        return failures

    def _get_init_script_data(self, init_script_info: InitScriptInfo) -> str | None:
        if init_script_info.dbfs is not None and init_script_info.dbfs.destination is not None:
            if len(init_script_info.dbfs.destination.split(":")) == INIT_SCRIPT_DBFS_PATH:
                file_api_format_destination = init_script_info.dbfs.destination.split(":")[1]
                if file_api_format_destination:
                    try:
                        data = self._ws.dbfs.read(file_api_format_destination).data
                        if data is not None:
                            return base64.b64decode(data).decode("utf-8")
                    except NotFound:
                        return None
        if init_script_info.workspace is not None and init_script_info.workspace.destination is not None:
            workspace_file_destination = init_script_info.workspace.destination
            try:
                data = self._ws.workspace.export(workspace_file_destination).content
                if data is not None:
                    return base64.b64decode(data).decode("utf-8")
            except NotFound:
                return None
        return None

    def _check_cluster_init_script(self, init_scripts: list[InitScriptInfo], source: str) -> list[str]:
        failures: list[str] = []
        for init_script_info in init_scripts:
            init_script_data = self._get_init_script_data(init_script_info)
            failures.extend(self.check_init_script(init_script_data, source))
        return failures

    def check_spark_conf(self, conf: dict[str, str], source: str) -> list[str]:
        failures: list[str] = []
        for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
            if k in conf:
                failures.append(f"unsupported config: {k}")
        for value in conf.values():
            if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                failures.append(f"using DBFS mount in configuration: {value}")
        # Checking if Azure cluster config is present in spark config
        if azure_sp_conf_present_check(conf):
            failures.append(f"{AZURE_SP_CONF_FAILURE_MSG} {source}.")
        return failures

    def check_cluster_failures(self, cluster: ClusterDetails, source: str) -> list[str]:
        failures: list[str] = []

        unsupported_cluster_types = [
            DataSecurityMode.LEGACY_PASSTHROUGH,
            DataSecurityMode.LEGACY_SINGLE_USER,
            DataSecurityMode.LEGACY_TABLE_ACL,
        ]
        support_status = spark_version_compatibility(cluster.spark_version)
        if support_status != "supported":
            failures.append(f"not supported DBR: {cluster.spark_version}")
        if cluster.spark_conf is not None:
            failures.extend(self.check_spark_conf(cluster.spark_conf, source))
        # Checking if Azure cluster config is present in cluster policies
        if cluster.policy_id is not None:
            failures.extend(self._check_cluster_policy(cluster.policy_id, source))
        if cluster.init_scripts is not None:
            failures.extend(self._check_cluster_init_script(cluster.init_scripts, source))
        if cluster.data_security_mode == DataSecurityMode.NONE:
            failures.append("No isolation shared clusters not supported in UC")
        if cluster.data_security_mode in unsupported_cluster_types:
            failures.append(f"cluster type not supported : {cluster.data_security_mode.value}")

        return failures


class ClustersCrawler(CrawlerBase[ClusterInfo], CheckClusterMixin):
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
                cluster_id=cluster.cluster_id if cluster.cluster_id else "",
                cluster_name=cluster.cluster_name,
                creator=cluster.creator_user_name,
                success=1,
                failures="[]",
            )
            failures = self.check_cluster_failures(cluster, "cluster")
            if len(failures) > 0:
                cluster_info.success = 0
                cluster_info.failures = json.dumps(failures)
            yield cluster_info

    def snapshot(self) -> Iterable[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ClusterInfo(*row)
