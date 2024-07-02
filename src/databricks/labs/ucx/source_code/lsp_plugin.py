from pylsp import hookimpl  # type: ignore
from pylsp.workspace import Document, Workspace  # type: ignore
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.lsp import Diagnostic


@hookimpl
def pylsp_lint(workspace: Workspace, document: Document) -> list[dict]:  # pylint: disable=unused-argument
    # TODO: initialize migration index and session state from config / env variables
    languages = LinterContext(index=None, session_state=None)
    analyser = languages.linter(Language.PYTHON)
    code = document.source
    diagnostics = [Diagnostic.from_advice(_) for _ in analyser.lint(code)]
    return [d.as_dict() for d in diagnostics]
