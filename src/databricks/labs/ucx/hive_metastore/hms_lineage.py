import base64
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import CreateResponse


class HiveMetastoreLineageEnabler:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def _get_init_script_content(self):
        _init_script_content = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  driver_conf=${DB_HOME}/driver/conf/spark-branch.conf
  if [ ! -e $driver_conf ] ; then
    touch $driver_conf
  fi
cat << EOF >>  $driver_conf
  [driver] {
   "spark.databricks.dataLineage.enabled" = true
   }
EOF
fi"""
        return base64.b64encode(_init_script_content.encode()).decode()

    def _add_global_init_script(self) -> CreateResponse:
        created_script = self._ws.global_init_scripts.create(
            name=f"hms-lineage-{time.time_ns()}", script=self._get_init_script_content(), enabled=True
        )
        return created_script

    def add_spark_config_for_hms_lineage(self):
        created_script = self._add_global_init_script()
        self._add_sql_wh_config()
        return created_script.script_id

    def _add_sql_wh_config(self):
        pass
