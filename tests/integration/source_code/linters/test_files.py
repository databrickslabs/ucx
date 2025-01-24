from io import StringIO
from pathlib import Path

import pytest
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.linters.context import LinterContext


@pytest.fixture
def local_checkout_context(ws) -> LocalCheckoutContext:
    context = LocalCheckoutContext(ws).replace(linter_context_factory=lambda session_state: LinterContext(TableMigrationIndex([]), session_state))
    return context


@pytest.fixture
def ucx_source_path() -> Path:
    ucx_path = Path(__file__).parent.parent.parent.parent.parent
    path = Path(ucx_path, "src")
    return path


def test_local_code_linter_lints_ucx(local_checkout_context, ucx_source_path) -> None:
    problems = local_checkout_context.local_code_linter.lint(Prompts(), ucx_source_path, StringIO())
    assert len(problems) > 0, "Found no problems while linting ucx"


def test_local_code_migrator_fixes_ucx(local_checkout_context, ucx_source_path) -> None:
    problems = local_checkout_context.local_code_migrator.apply(Prompts(), ucx_source_path)
    assert len(problems) > 0, "Found no problems while linting ucx"
