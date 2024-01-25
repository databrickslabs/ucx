import base64
import json
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.crawlers import (
    _AZURE_SP_CONF_FAILURE_MSG,
    _azure_sp_conf_in_init_scripts,
    _check_init_script,
    logger,
)
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend


@dataclass
class GlobalInitScriptInfo:
    script_id: str
    success: int
    failures: str
    script_name: str | None = None
    created_by: str | None = None
    enabled: bool | None = None


class GlobalInitScriptCrawler(CrawlerBase[GlobalInitScriptInfo]):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "global_init_scripts", GlobalInitScriptInfo)
        self._ws = ws

    def _crawl(self) -> Iterable[GlobalInitScriptInfo]:
        all_global_init_scripts = list(self._ws.global_init_scripts.list())
        return list(self._assess_global_init_scripts(all_global_init_scripts))

    def _assess_global_init_scripts(self, all_global_init_scripts):
        for gis in all_global_init_scripts:
            if not gis.created_by:
                logger.warning(
                    f"Script {gis.name} have Unknown creator, it means that the original creator has been deleted"
                    f" and should be re-created"
                )
            global_init_script_info = GlobalInitScriptInfo(
                script_id=gis.script_id,
                script_name=gis.name,
                created_by=gis.created_by,
                enabled=gis.enabled,
                success=1,
                failures="[]",
            )
            failures = []
            script = self._ws.global_init_scripts.get(gis.script_id)
            global_init_script = base64.b64decode(script.script).decode("utf-8")
            if not global_init_script:
                continue
            failures.extend(_check_init_script(global_init_script, "global init script"))
            global_init_script_info.failures = json.dumps(failures)

            if len(failures) > 0:
                global_init_script_info.success = 0
            yield global_init_script_info

    def snapshot(self) -> Iterable[GlobalInitScriptInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[GlobalInitScriptInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield GlobalInitScriptInfo(*row)
