import logging
from packaging import version

from pylsp import hookimpl  # type: ignore
from pylsp.config.config import Config  # type: ignore
from pylsp.workspace import Document  # type: ignore
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableMigrationStatus
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.lsp import Diagnostic


logger = logging.getLogger(__name__)


@hookimpl
def pylsp_lint(config: Config, document: Document) -> list[dict]:
    cfg = config.plugin_settings('pylsp_ucx', document_path=document.uri)

    migration_index = TableMigrationIndex([TableMigrationStatus.from_json(st) for st in cfg.get('migration_index', [])])

    session_state = CurrentSessionState(
        data_security_mode=CurrentSessionState.parse_security_mode(cfg.get('dataSecurityMode', None)),
        dbr_version=parse_dbr_version(cfg.get('dbrVersion', None)),
        is_serverless=bool(cfg.get('isServerless', False)),
    )
    languages = LinterContext(index=migration_index, session_state=session_state)
    analyser = languages.linter(Language.PYTHON)
    code = document.source
    diagnostics = [Diagnostic.from_advice(_) for _ in analyser.lint(code)]
    return [d.as_dict() for d in diagnostics]


def parse_dbr_version(version_str: str | None) -> tuple[int, int] | None:
    if not version_str:
        return None
    try:
        release_version = version.parse(version_str).release
        return release_version[0], release_version[1]
    except version.InvalidVersion:
        logger.warning(f'Incorrect DBR version string: {version_str}')
        return None
