import logging

from pylsp import hookimpl  # type: ignore
from pylsp.config.config import Config  # type: ignore
from pylsp.workspace import Document  # type: ignore
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.lsp import Diagnostic
from databricks.sdk.service.compute import DataSecurityMode


logger = logging.getLogger(__name__)


@hookimpl
def pylsp_lint(config: Config, document: Document) -> list[dict]:
    cfg = config.plugin_settings('pylsp_ucx', document_path=document.uri)

    session_state = CurrentSessionState(
        data_security_mode=parse_data_security_mode(cfg.get('dataSecurityMode', None)),
        dbr_version=parse_dbr_version(cfg.get('dbrVersion', None)),
        is_serverless=bool(cfg.get('isServerless', False)),
    )
    languages = LinterContext(index=None, session_state=session_state)
    analyser = languages.linter(Language.PYTHON)
    code = document.source
    diagnostics = [Diagnostic.from_advice(_) for _ in analyser.lint(code)]
    return [d.as_dict() for d in diagnostics]


def parse_dbr_version(version: str | None) -> tuple[int, int] | None:
    if not version:
        return None
    version_parts = [int(i) for i in version.split('.') if i.isnumeric()]
    if len(version_parts) < 2:
        logger.error(f'Incorrect DBR version string: {version}')
        return None
    return version_parts[0], version_parts[1]


def parse_data_security_mode(mode_str: str | None):
    if not mode_str:
        return None
    try:
        return DataSecurityMode(mode_str)
    except ValueError as _:
        logger.error(f'Unknown data_security_mode: {mode_str}')
        return None
