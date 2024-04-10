import base64
import logging
import time

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service.compute import GlobalInitScriptDetailsWithContent

GLOBAL_INIT_SCRIPT = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
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

logger = logging.getLogger(__name__)


class HiveMetastoreLineageEnabler:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def apply(self, prompts: Prompts, is_account_install: bool = False):
        script = self._check_lineage_spark_config_exists()
        if script:
            if script.enabled:
                logger.info("HMS lineage init script already exists and enabled")
                return
            if is_account_install or prompts.confirm(
                "HMS lineage collection init script is disabled, do you want to enable it?"
            ):
                logger.info("Enabling Global Init Script...")
                self._enable_global_init_script(script)
            return
        logger.info(
            "HMS Lineage feature creates one system table named system.hms_to_uc_migration.table_access and "
            "helps in your migration process from HMS to UC by allowing you to programmatically query HMS "
            "lineage data."
        )
        if is_account_install or prompts.confirm(
            "No HMS lineage collection init script exists, do you want to create one?"
        ):
            logger.info("Creating Global Init Script...")
            self._add_global_init_script()

    def _check_lineage_spark_config_exists(self) -> GlobalInitScriptDetailsWithContent | None:
        for script in self._ws.global_init_scripts.list():
            if not script.script_id:
                continue
            try:
                script_content = self._ws.global_init_scripts.get(script_id=script.script_id)
            except InvalidParameterValue as err:
                logger.warning(f"Failed to get init script {script.script_id}: {err}")
                continue
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
        return base64.b64encode(GLOBAL_INIT_SCRIPT.encode()).decode()

    def _add_global_init_script(self) -> str:
        content = self._get_init_script_content()
        created_script = self._ws.global_init_scripts.create(f"hms-lineage-{time.time_ns()}", content, enabled=True)
        assert created_script.script_id is not None
        return created_script.script_id

    def _enable_global_init_script(self, gscript: GlobalInitScriptDetailsWithContent) -> str:
        script_id = gscript.script_id
        assert script_id is not None
        assert gscript.name is not None
        assert gscript.script is not None
        self._ws.global_init_scripts.update(script_id, gscript.name, gscript.script, enabled=True)
        return script_id
