import logging
import os

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service.compute import DataSecurityMode, AutoScale
logger = logging.getLogger(__name__)


class ClusterAccess:
    def __init__(self, prompts: Prompts, ws: WorkspaceClient):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prompts = prompts

    @classmethod
    def current(cls, ws: WorkspaceClient):
        prompts = Prompts()
        return ClusterAccess(prompts, ws)

    def map_cluster_to_uc(self, cluster_id):
        try:
            spark_version = self._ws.clusters.select_spark_version(latest=True)
            node_type_id = self._ws.clusters.select_node_type(local_disk=True)
            security_modes = {"Single User": DataSecurityMode.SINGLE_USER, "Shared": DataSecurityMode.USER_ISOLATION}
            access_mode = self._prompts.choice_from_dict("Select cluster access mode", security_modes)
            cluster_details = self._ws.clusters.get(cluster_id)
            data_security_mode = access_mode
            self._ws.clusters.edit(
                cluster_id=cluster_id,
                cluster_name=cluster_details.cluster_name,
                spark_version=spark_version,
                spark_conf=cluster_details.spark_conf,
                spark_env_vars=cluster_details.spark_env_vars,
                data_security_mode=data_security_mode,
                node_type_id=node_type_id,
                autoscale=cluster_details.autoscale,
                policy_id=cluster_details.policy_id,
                autotermination_minutes=cluster_details.autotermination_minutes,

            )
        except InvalidParameterValue as e:
            logger.warning(f"skipping cluster remapping: {e}")