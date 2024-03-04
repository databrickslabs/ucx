import logging
import os

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service.compute import DataSecurityMode
logger = logging.getLogger(__name__)


class ClusterAccess:
    def __init__(self, prompts: Prompts, ws: WorkspaceClient):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prompts = prompts

    def map_cluster_to_uc(self, cluster_id):
        try:
            spark_version = self._ws.clusters.select_spark_version(latest=True)
            security_modes = {"Single User": DataSecurityMode.SINGLE_USER, "Shared": DataSecurityMode.USER_ISOLATION}
            access_mode = self._prompts.choice_from_dict("Select cluster access mode", security_modes)
            if access_mode == 'SINGLE_USER':
                data_security_mode = DataSecurityMode.SINGLE_USER
            else:
                data_security_mode = DataSecurityMode.USER_ISOLATION
            self._ws.clusters.edit(
                cluster_id=cluster_id, spark_version=spark_version, data_security_mode=data_security_mode
            )
        except InvalidParameterValue as e:
            logger.warning(f"skipping cluster remapping: {e}")