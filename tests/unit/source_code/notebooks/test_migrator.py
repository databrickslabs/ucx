from pathlib import Path

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.migrator import NotebookMigrator


def test_notebook_migrator_ignores_unsupported_extensions() -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = NotebookMigrator(languages)
    path = Path('unsupported.ext')
    assert not migrator.apply(path)


def test_notebook_migrator_supported_language_no_diagnostics(mock_path_lookup) -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = NotebookMigrator(languages)
    path = mock_path_lookup.resolve(Path("root1.run.py"))
    assert not migrator.apply(path)
