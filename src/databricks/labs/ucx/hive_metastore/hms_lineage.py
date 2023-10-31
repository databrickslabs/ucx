import base64
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import GlobalInitScriptDetailsWithContent


class HiveMetastoreLineageEnabler:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def check_lineage_spark_config_exists(self) -> GlobalInitScriptDetailsWithContent:
        for script in self._ws.global_init_scripts.list():
            gscript = self._ws.global_init_scripts.get(script_id=script.script_id)
            if not gscript:
                continue
            if "spark.databricks.dataLineage.enabled" in base64.b64decode(gscript.script).decode("utf-8"):
                return gscript

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

    def add_global_init_script(self) -> str:
        created_script = self._ws.global_init_scripts.create(
            name=f"hms-lineage-{time.time_ns()}", script=self._get_init_script_content(), enabled=True
        )
        return created_script.script_id

    def enable_global_init_script(self, gscript: GlobalInitScriptDetailsWithContent) -> str:
        self._ws.global_init_scripts.update(
            script_id=gscript.script_id, name=gscript.name, script=gscript.script, enabled=True
        )
        return gscript.script_id
