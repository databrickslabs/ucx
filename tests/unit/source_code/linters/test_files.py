from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts


from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState, Deprecation, Failure
from databricks.labs.ucx.source_code.files import FileLoader, LocalFile, ImportFileResolver
from databricks.labs.ucx.source_code.folders import FolderLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyResolver, SourceContainer
from databricks.labs.ucx.source_code.linters.base import PythonLinter
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.files import FileLinter, NotebookLinter, NotebookMigrator
from databricks.labs.ucx.source_code.linters.folders import LocalCodeLinter
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver

from tests.unit import locate_site_packages


def test_file_linter_lints_file() -> None:
    local_file = create_autospec(LocalFile)
    local_file.language = Language.PYTHON
    local_file.original_code = "print(1)"
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = local_file
    path_lookup = create_autospec(PathLookup)
    python_linter = create_autospec(PythonLinter)
    context = create_autospec(LinterContext)
    context.linter.return_value = python_linter
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert not advices
    local_file.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.linter.assert_called_once_with(Language.PYTHON)
    python_linter.lint.assert_called_once_with("print(1)")


def test_file_linter_lints_python(tmp_path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / "xyz.py"
    path.write_text("a = 3")
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices


def test_file_linter_lints_sql(tmp_path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / "xyz.sql"
    path.write_text("SELECT * FROM dual")
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices


def test_file_linter_lints_notebook() -> None:
    notebook = create_autospec(Notebook)
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = notebook
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert not advices
    notebook.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.assert_not_called()


def test_file_linter_lints_unsupported_file() -> None:
    source_container = create_autospec(SourceContainer)
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = source_container
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert advices == [Failure("unsupported-file", "Unsupported file", -1, -1, -1, -1)]
    source_container.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.assert_not_called()


def test_file_linter_lints_this_file(mock_path_lookup) -> None:
    """This test does not mock to test closer to reality."""
    dependency = Dependency(FileLoader(), Path(__file__), inherits_context=False)
    context = LinterContext()
    linter = FileLinter(dependency, mock_path_lookup, context)

    advices = list(linter.lint())

    assert not advices


def test_file_linter_applies_unsupported_file() -> None:
    source_container = create_autospec(SourceContainer)
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = source_container
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    linter.apply()

    source_container.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.assert_not_called()


def test_file_linter_applies_migrated(tmp_path, mock_path_lookup, migration_index) -> None:
    """This test does not mock to test closer to reality."""
    path = tmp_path / "file.py"
    path.write_text("df = spark.read.table('old.things')")
    dependency = Dependency(FileLoader(), path, inherits_context=False)
    context = LinterContext(migration_index)
    linter = FileLinter(dependency, mock_path_lookup, context)

    linter.apply()

    # The .rstrip() is to remove the trailing newlines added by the fixer
    assert path.read_text().rstrip() == "df = spark.read.table('brand.new.stuff')"


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


def test_triple_dot_import() -> None:
    file_resolver = ImportFileResolver(FileLoader())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, "...foo")
    assert not maybe.problems
    assert maybe.dependency is not None
    assert maybe.dependency.path == Path('/some/path/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/../../foo.py'))


def test_single_dot_import() -> None:
    file_resolver = ImportFileResolver(FileLoader())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/to/folder/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, ".foo")
    assert not maybe.problems
    assert maybe.dependency is not None
    assert maybe.dependency.path == Path('/some/path/to/folder/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/foo.py'))


site_packages = locate_site_packages()


@pytest.mark.skip("Manual testing for troubleshooting")
@pytest.mark.parametrize(
    "path", [Path("/Users/eric.vergnaud/development/ucx/.venv/lib/python3.10/site-packages/spacy/pipe_analysis.py")]
)
def test_known_issues(path: Path, migration_index) -> None:
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    folder_loader = FolderLoader(notebook_loader, file_loader)
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    session_state = CurrentSessionState()
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(file_loader)
    pip_resolver = PythonLibraryResolver()
    resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, import_resolver, path_lookup)
    linter = LocalCodeLinter(
        notebook_loader,
        file_loader,
        folder_loader,
        path_lookup,
        session_state,
        resolver,
        lambda: LinterContext(migration_index, session_state),
    )
    advices = linter.lint(MockPrompts({}), path)
    for advice in advices:
        print(repr(advice))


def test_notebook_linter_lints_parent_child_context_from_grand_parent(migration_index, mock_path_lookup) -> None:
    """Verify the NotebookLinter can resolve %run"""
    path = Path(__file__).parent.parent / "samples" / "parent-child-context" / "grand_parent.py"
    notebook = Notebook.parse(path, path.read_text(), Language.PYTHON)
    linter = NotebookLinter(notebook, mock_path_lookup.change_directory(path.parent), LinterContext(migration_index))

    advices = list(linter.lint())

    assert not advices, "Expected no advices"


class _NotebookLinter(NotebookLinter):
    """A helper class to construct the notebook linter from source code for testing simplification."""

    @classmethod
    def from_source_code(
        cls, index: TableMigrationIndex, path_lookup: PathLookup, source: str, default_language: Language
    ) -> NotebookLinter:
        context = LinterContext(index)
        notebook = Notebook.parse(Path(""), source, default_language)
        assert notebook is not None
        return cls(notebook, path_lookup, context)


def test_notebook_linter_lints_source_yielding_no_advices(migration_index, mock_path_lookup) -> None:
    linter = _NotebookLinter.from_source_code(
        migration_index, mock_path_lookup, "# Databricks notebook source\nprint(1)\n", Language.PYTHON
    )

    advices = list(linter.lint())

    assert not advices, "Expected no advices"


def test_notebook_linter_lints_source_yielding_parse_failure(migration_index, mock_path_lookup) -> None:
    linter = _NotebookLinter.from_source_code(
        migration_index, mock_path_lookup, "# Databricks notebook source\nprint(1\n", Language.PYTHON
    )

    advices = list(linter.lint())

    assert advices == [
        Failure(
            code='python-parse-error',
            message='Failed to parse code due to invalid syntax: print(1',
            start_line=1,
            start_col=5,
            end_line=1,
            end_col=1,
        )
    ]


@pytest.mark.xfail(reason="https://github.com/databrickslabs/ucx/issues/3556")
def test_notebook_linter_lints_source_yielding_parse_failures(migration_index, mock_path_lookup) -> None:
    source = """
# Databricks notebook source

print(1

# COMMAND ----------

print(2
""".lstrip()  # Missing parentheses is on purpose
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert advices == [
        Failure(
            code='python-parse-error',
            message='Failed to parse code due to invalid syntax: print(1',
            start_line=2,
            start_col=5,
            end_line=2,
            end_col=1,
        ),
        Failure(
            code='python-parse-error',
            message='Failed to parse code due to invalid syntax: print(2',
            start_line=6,
            start_col=5,
            end_line=6,
            end_col=1,
        ),
    ]


def test_notebook_linter_lints_migrated_table(migration_index, mock_path_lookup) -> None:
    """Regression test with the tests below."""
    source = """
# Databricks notebook source

table_name = "old.things"  # Migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert advices
    assert advices[0] == Deprecation(
        code='table-migrated-to-uc-python',
        message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
        start_line=6,
        start_col=0,
        end_line=6,
        end_col=23,
    )


def test_notebook_linter_lints_not_migrated_table(migration_index, mock_path_lookup) -> None:
    """Regression test with the tests above and below."""
    source = """
# Databricks notebook source

table_name = "not_migrated.table"  # NOT a migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert not [advice for advice in advices if advice.code == "table-migrated-to-uc"]


def test_notebook_linter_lints_migrated_table_with_rename(migration_index, mock_path_lookup) -> None:
    """The spark.table should read the table defined above the call not below.

    This is a regression test with the tests above and below.
    """
    source = """
# Databricks notebook source

table_name = "old.things"  # Migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)

# COMMAND ----------

table_name = "not_migrated.table"  # NOT a migrated table according to the migration index
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    first_advice = next(iter(linter.lint()))

    assert first_advice == Deprecation(
        code='table-migrated-to-uc-python',
        message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
        start_line=6,
        start_col=0,
        end_line=6,
        end_col=23,
    )


def test_notebook_linter_lints_not_migrated_table_with_rename(migration_index, mock_path_lookup) -> None:
    """The spark.table should read the table defined above the call not below.

    This is a regression test with the tests above.
    """
    source = """
# Databricks notebook source

table_name = "not_migrated.table"  # NOT a migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)

# COMMAND ----------

table_name = "old.things"  # Migrated table according to the migration index
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert not [advice for advice in advices if advice.code == "table-migrated-to-uc"]
