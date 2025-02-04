import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import Fixer, Linter
from databricks.labs.ucx.source_code.linters.context import LinterContext


def test_linter_context_linter_returns_correct_analyser_for_python() -> None:
    context = LinterContext(TableMigrationIndex([]))
    linter = context.linter(Language.PYTHON)
    assert isinstance(linter, Linter)


def test_linter_context_linter_returns_correct_analyser_for_sql() -> None:
    context = LinterContext(TableMigrationIndex([]))
    linter = context.linter(Language.SQL)
    assert isinstance(linter, Linter)


def test_linter_context_linter_raises_error_for_unsupported_language() -> None:
    context = LinterContext(TableMigrationIndex([]))
    with pytest.raises(ValueError):
        context.linter(Language.R)


def test_linter_context_fixer_returns_none_fixer_for_python() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.PYTHON, "diagnostic_code")
    assert fixer is None


def test_linter_context_fixer_returns_correct_fixer_for_python() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.PYTHON, "table-migrated-to-uc-python")
    assert isinstance(fixer, Fixer)


def test_linter_context_fixer_returns_none_fixer_for_sql() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.SQL, "diagnostic_code")
    assert fixer is None


def test_linter_context_fixer_returns_correct_fixer_for_sql() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.SQL, "table-migrated-to-uc-sql")
    assert isinstance(fixer, Fixer)


def test_linter_context_fixer_returns_none_for_unsupported_language() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.SCALA, "diagnostic_code")
    assert fixer is None


def test_linter_context_linter_apply_fixes_no_operation() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixed_code = context.apply_fixes(Language.PYTHON, "print(1)")
    assert fixed_code == "print(1)"
