import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service.compute import ClusterDetails, DataSecurityMode

logger = logging.getLogger(__name__)


class ClusterAccess:
    def __init__(self, installation: Installation, ws: WorkspaceClient, prompts: Prompts):
        self._ws = ws
        self._prompts = prompts
        self._installation = installation

    def list_cluster(self):
        clusters = []
        for cluster_info in self._ws.clusters.list():
            if cluster_info.cluster_source is None:
                continue
            if cluster_info.cluster_source.name == "JOB":
                continue
            clusters.append(cluster_info)
        return clusters

    def _get_access_mode(self, access_mode: str):
        if access_mode in {"LEGACY_SINGLE_USER", "SINGLE_USER"}:
            return DataSecurityMode.SINGLE_USER
        return DataSecurityMode.USER_ISOLATION

    def map_cluster_to_uc(self, cluster_id: str, cluster_details: list[ClusterDetails]):
        if cluster_id != "<ALL>":
            cluster_ids = [x.strip() for x in cluster_id.split(",")]
            cluster_id_list = [cluster for cluster in cluster_details if cluster.cluster_id in cluster_ids]
        else:
            cluster_id_list = cluster_details
        spark_version = self._ws.clusters.select_spark_version(latest=True, long_term_support=True)
        for cluster in cluster_id_list:
            try:
                assert cluster.cluster_id is not None
                if cluster.data_security_mode is None:
                    raise InvalidParameterValue(f"Data security Mode is None for the cluster {cluster.cluster_id}")
                access_mode = self._get_access_mode(cluster.data_security_mode.name)
                self._installation.save(cluster, filename=f'backup/clusters/{cluster.cluster_id}.json')
                logger.info(f"Editing the cluster of cluster: {cluster.cluster_id} with access_mode as {access_mode}")
                self._ws.clusters.edit(
                    cluster_id=cluster.cluster_id,
                    cluster_name=cluster.cluster_name,
                    spark_version=spark_version,
                    num_workers=cluster.num_workers,
                    spark_conf=cluster.spark_conf,
                    spark_env_vars=cluster.spark_env_vars,
                    data_security_mode=access_mode,
                    node_type_id=cluster.node_type_id,
                    autoscale=cluster.autoscale,
                    policy_id=cluster.policy_id,
                    autotermination_minutes=cluster.autotermination_minutes,
                    custom_tags=cluster.custom_tags,
                    init_scripts=cluster.init_scripts,
                    cluster_log_conf=cluster.cluster_log_conf,
                    aws_attributes=cluster.aws_attributes,
                    ssh_public_keys=cluster.ssh_public_keys,
                    enable_elastic_disk=cluster.enable_elastic_disk,
                    instance_pool_id=cluster.instance_pool_id,
                    enable_local_disk_encryption=cluster.enable_local_disk_encryption,
                    driver_instance_pool_id=cluster.driver_instance_pool_id,
                )
            except InvalidParameterValue as e:
                logger.warning(f"skipping cluster remapping: {e}")

    def revert_cluster_remap(self, cluster_ids: str, total_cluster_ids: list):
        if cluster_ids != "<ALL>":
            cluster_list = [x.strip() for x in cluster_ids.split(",")]
        else:
            cluster_list = total_cluster_ids
        logger.info(f"Reverting the configurations for the cluster {cluster_list}")
        for cluster in cluster_list:
            try:
                cluster_details = self._installation.load(ClusterDetails, filename=f"backup/clusters/{cluster}.json")
                if cluster_details.spark_version is None:
                    raise InvalidParameterValue(
                        f"Spark Version is not present in the config file for the cluster:{cluster}"
                    )
                if cluster_details.cluster_id is None:
                    raise InvalidParameterValue(
                        f"cluster Id is not present in the config file for the cluster:{cluster}"
                    )
                num_workers = cluster_details.num_workers if cluster_details.num_workers else 0
                self._ws.clusters.edit(
                    cluster_id=cluster_details.cluster_id,
                    cluster_name=cluster_details.cluster_name,
                    spark_version=cluster_details.spark_version,
                    num_workers=num_workers,
                    spark_conf=cluster_details.spark_conf,
                    spark_env_vars=cluster_details.spark_env_vars,
                    data_security_mode=cluster_details.data_security_mode,
                    node_type_id=cluster_details.node_type_id,
                    autoscale=cluster_details.autoscale,
                    policy_id=cluster_details.policy_id,
                    autotermination_minutes=cluster_details.autotermination_minutes,
                    custom_tags=cluster_details.custom_tags,
                    init_scripts=cluster_details.init_scripts,
                    cluster_log_conf=cluster_details.cluster_log_conf,
                    aws_attributes=cluster_details.aws_attributes,
                    ssh_public_keys=cluster_details.ssh_public_keys,
                    enable_elastic_disk=cluster_details.enable_elastic_disk,
                    instance_pool_id=cluster_details.instance_pool_id,
                    enable_local_disk_encryption=cluster_details.enable_local_disk_encryption,
                    driver_instance_pool_id=cluster_details.driver_instance_pool_id,
                )
            except InvalidParameterValue as e:
                logger.warning(f"skipping cluster remapping: {e}")
