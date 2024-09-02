import base64
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist

from databricks.labs.ucx.assessment.crawlers import (
    AZURE_SP_CONF_FAILURE_MSG,
    azure_sp_conf_in_init_scripts,
)
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.framework.crawlers import CrawlerBase

logger = logging.getLogger(__name__)


@dataclass
class GlobalInitScriptInfo:
    script_id: str
    success: int
    failures: str
    script_name: str | None = None
    created_by: str | None = None
    enabled: bool | None = None


class CheckInitScriptMixin:
    _ws: WorkspaceClient

    def check_init_script(self, init_script_data: str | None, source: str) -> list[str]:
        failures: list[str] = []
        if not init_script_data:
            return failures
        if azure_sp_conf_in_init_scripts(init_script_data):
            failures.append(f"{AZURE_SP_CONF_FAILURE_MSG} {source}.")
        return failures


class GlobalInitScriptCrawler(CrawlerBase[GlobalInitScriptInfo], CheckInitScriptMixin):
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
            try:
                script = self._ws.global_init_scripts.get(gis.script_id)
            except ResourceDoesNotExist:
                logger.warning(f"removed on the backend {gis.script_id}")
                continue
            global_init_script = base64.b64decode(script.script).decode("utf-8")
            if not global_init_script:
                continue
            failures.extend(self.check_init_script(global_init_script, "global init script"))
            global_init_script_info.failures = json.dumps(failures)

            if len(failures) > 0:
                global_init_script_info.success = 0
            yield global_init_script_info

    def _try_fetch(self) -> Iterable[GlobalInitScriptInfo]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield GlobalInitScriptInfo(*row)
