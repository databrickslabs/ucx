from io import StringIO
from pathlib import Path

from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.files import LocalCodeLinter
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.linters.context import LinterContext


def test_local_code_linter_lints_ucx(simple_ctx) -> None:
    session_state = CurrentSessionState()  # No need to connect
    linter_context = LinterContext(TableMigrationIndex([]), session_state)
    ucx_path = Path(__file__).parent.parent.parent.parent.parent
    path_to_scan = Path(ucx_path, "src")
    # TODO: LocalCheckoutContext has to move into GlobalContext because of this hack
    linter = LocalCodeLinter(
        simple_ctx.notebook_loader,
        simple_ctx.file_loader,
        simple_ctx.folder_loader,
        simple_ctx.path_lookup,
        session_state,
        simple_ctx.dependency_resolver,
        lambda: linter_context,
    )
    problems = linter.lint(Prompts(), path_to_scan, StringIO())
    assert len(problems) > 0, f"Found problems while linting ucx: {problems}"
