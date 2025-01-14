from io import StringIO
from pathlib import Path

import pytest
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.files import LocalCodeLinter, LocalCodeMigrator
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.linters.context import LinterContext


@pytest.fixture
def ucx_source_path() -> Path:
    ucx_path = Path(__file__).parent.parent.parent.parent.parent
    path = Path(ucx_path, "src")
    return path


def test_local_code_linter_lints_ucx(simple_ctx, ucx_source_path) -> None:
    session_state = CurrentSessionState()  # No need to connect
    linter_context = LinterContext(TableMigrationIndex([]), session_state)
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
    problems = linter.lint(Prompts(), ucx_source_path, StringIO())
    assert len(problems) > 0, "Found no problems while linting ucx"


def test_local_code_migrator_fixes_ucx(simple_ctx, ucx_source_path) -> None:
    session_state = CurrentSessionState()  # No need to connect
    linter_context = LinterContext(TableMigrationIndex([]), session_state)
    migrator = LocalCodeMigrator(lambda: linter_context)

    has_code_changes = migrator.apply(ucx_source_path)

    assert not has_code_changes
