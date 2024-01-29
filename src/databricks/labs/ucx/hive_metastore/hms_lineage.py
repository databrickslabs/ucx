import base64
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import GlobalInitScriptDetailsWithContent

global_init_script = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
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


class HiveMetastoreLineageEnabler:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def check_lineage_spark_config_exists(self) -> GlobalInitScriptDetailsWithContent | None:
        for script in self._ws.global_init_scripts.list():
            if not script.script_id:
                continue
            script_content = self._ws.global_init_scripts.get(script_id=script.script_id)
            if not script_content:
                continue
            content = script_content.script
            if not content:
                continue
            if "spark.databricks.dataLineage.enabled" in base64.b64decode(content).decode("utf-8"):
                return script_content
        return None

    @staticmethod
    def _get_init_script_content():
        return base64.b64encode(global_init_script.encode()).decode()

    def add_global_init_script(self) -> str:
        content = self._get_init_script_content()
        created_script = self._ws.global_init_scripts.create(f"hms-lineage-{time.time_ns()}", content, enabled=True)
        assert created_script.script_id is not None
        return created_script.script_id

    def enable_global_init_script(self, gscript: GlobalInitScriptDetailsWithContent) -> str:
        script_id = gscript.script_id
        assert script_id is not None
        assert gscript.name is not None
        assert gscript.script is not None
        self._ws.global_init_scripts.update(script_id, gscript.name, gscript.script, enabled=True)
        return script_id
