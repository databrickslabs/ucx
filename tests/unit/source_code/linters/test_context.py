import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import Fixer, Linter
from databricks.labs.ucx.source_code.linters.context import LinterContext


@pytest.fixture
def empty_table_migration_index() -> TableMigrationIndex:
    index = TableMigrationIndex([])
    return index


def test_linter_returns_correct_analyser_for_python(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    linter = languages.linter(Language.PYTHON)
    assert isinstance(linter, Linter)


def test_linter_returns_correct_analyser_for_sql(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    linter = languages.linter(Language.SQL)
    assert isinstance(linter, Linter)


def test_linter_raises_error_for_unsupported_language(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    with pytest.raises(ValueError):
        languages.linter(Language.R)


def test_fixer_returns_none_fixer_for_python(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    fixer = languages.fixer(Language.PYTHON, "diagnostic_code")
    assert fixer is None


def test_fixer_returns_correct_fixer_for_python(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    fixer = languages.fixer(Language.PYTHON, "table-migrate")
    assert isinstance(fixer, Fixer)


def test_fixer_returns_none_fixer_for_sql(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    fixer = languages.fixer(Language.SQL, "diagnostic_code")
    assert fixer is None


def test_fixer_returns_correct_fixer_for_sql(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    fixer = languages.fixer(Language.SQL, "table-migrate")
    assert isinstance(fixer, Fixer) or fixer is None


def test_fixer_returns_none_for_unsupported_language(empty_table_migration_index: TableMigrationIndex) -> None:
    languages = LinterContext(empty_table_migration_index)
    fixer = languages.fixer(Language.SCALA, "diagnostic_code")
    assert fixer is None


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
