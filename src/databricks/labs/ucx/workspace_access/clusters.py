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
        cluster_list = {}
        clusters = self._ws.clusters.list()
        for cluster in clusters:
            if cluster.cluster_source is not None and cluster.cluster_source.name != "JOB":
                cluster_list[cluster.cluster_name] = cluster.cluster_id
        return cluster_list

    def _get_cluster_id(self, cluster_id:str):
        if cluster_id != "<ALL>":
            return [x.strip() for x in cluster_id.split(",")]
        cluster_list = []
        clusters = self._ws.clusters.list()
        for cluster in clusters:
            if cluster.cluster_source is not None and cluster.cluster_source.name != "JOB":
                cluster_list.append(cluster.cluster_id)
        return cluster_list

    def map_cluster_to_uc(self, cluster_id: str):

        cluster_id_list=self._get_cluster_id(cluster_id)
        spark_version = self._ws.clusters.select_spark_version(latest=True)
        security_modes = {"Single User": DataSecurityMode.SINGLE_USER, "Shared": DataSecurityMode.USER_ISOLATION}
        access_mode = self._prompts.choice_from_dict("Select cluster access mode", security_modes)
        try:
            for cluster_id in cluster_id_list:
                if cluster_id is None:
                    msg = "Cluster Id is not Provided. Please provide the cluster id"
                    raise InvalidParameterValue(msg)
                cluster_details = self._ws.clusters.get(cluster_id)
                self._installation.save(cluster_details, filename=f'backup/clusters/{cluster_details.cluster_id}.json')
                self._ws.clusters.edit(
                    cluster_id=cluster_id,
                    cluster_name=cluster_details.cluster_name,
                    spark_version=spark_version,
                    num_workers=cluster_details.num_workers,
                    spark_conf=cluster_details.spark_conf,
                    spark_env_vars=cluster_details.spark_env_vars,
                    data_security_mode=access_mode,
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
                    cluster_source=cluster_details.cluster_source,
                    instance_pool_id=cluster_details.instance_pool_id,
                    enable_local_disk_encryption=cluster_details.enable_local_disk_encryption,
                    driver_instance_pool_id=cluster_details.driver_instance_pool_id,
                )
        except InvalidParameterValue as e:
            logger.warning(f"skipping cluster remapping: {e}")

    def revert_cluster_remap(self, cluster_id: str | None = None):
        try:
            if cluster_id is None:
                raise InvalidParameterValue("Cluster Id is not Provided. Please provide the cluster id")
            cluster_details = self._installation.load(ClusterDetails, filename=f'backup/clusters/{cluster_id}.json')
            if cluster_details.spark_version is None:
                raise InvalidParameterValue("cluster does not have spark version")
            self._ws.clusters.edit(
                cluster_id=cluster_id,
                cluster_name=cluster_details.cluster_name,
                spark_version=cluster_details.spark_version,
                num_workers=cluster_details.num_workers,
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
                cluster_source=cluster_details.cluster_source,
                instance_pool_id=cluster_details.instance_pool_id,
                enable_local_disk_encryption=cluster_details.enable_local_disk_encryption,
                driver_instance_pool_id=cluster_details.driver_instance_pool_id,
            )
        except InvalidParameterValue as e:
            logger.warning(f"skipping cluster remapping: {e}")
