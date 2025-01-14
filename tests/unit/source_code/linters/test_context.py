import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import Fixer, Linter
from databricks.labs.ucx.source_code.linters.context import LinterContext


def test_linter_context_has_unique_fixer_names() -> None:
    try:
        LinterContext(TableMigrationIndex([]))
    except NameError:
        assert False, "Fixers should have unique names"
    else:
        assert True, "Fixers have unique names"


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
    fixer = context.fixer(Language.PYTHON, "table-migrate")
    assert isinstance(fixer, Fixer)


def test_linter_context_fixer_returns_none_fixer_for_sql() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.SQL, "diagnostic_code")
    assert fixer is None


def test_linter_context_fixer_returns_correct_fixer_for_sql() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.SQL, "table-migrate")
    assert isinstance(fixer, Fixer) or fixer is None


def test_linter_context_fixer_returns_none_for_unsupported_language() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixer = context.fixer(Language.SCALA, "diagnostic_code")
    assert fixer is None


def test_linter_context_linter_apply_fixes_no_operation() -> None:
    context = LinterContext(TableMigrationIndex([]))
    fixed_code = context.apply_fixes(Language.PYTHON, "print(1)")
    assert fixed_code == "print(1)"


@pytest.mark.parametrize(
    'code,expected',
    [
        ('spark.table("a.b").count()', {'r:a.b'}),
        ('spark.getTable("a.b")', {'r:a.b'}),
        ('spark.cacheTable("a.b")', {'r:a.b'}),
        ('spark.range(10).saveAsTable("a.b")', {'w:a.b'}),
        ('spark.sql("SELECT * FROM b.c LEFT JOIN c.d USING (e)")', {'r:b.c', 'r:c.d'}),
        ('spark.sql("SELECT * FROM delta.`/foo/bar`")', set()),
    ],
)
def test_collector_walker_from_python(code, expected, migration_index) -> None:
    used = set()
    ctx = LinterContext(migration_index)
    collector = ctx.tables_collector(Language.PYTHON)
    for used_table in collector.collect_tables(code):
        prefix = 'r' if used_table.is_read else 'w'
        used.add(f'{prefix}:{used_table.schema_name}.{used_table.table_name}')
    assert used == expected
