import io
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableMigrationStatus
from databricks.labs.ucx.source_code.base import Advice, CurrentSessionState, Deprecation, LocatedAdvice
from databricks.labs.ucx.source_code.graph import DependencyResolver, SourceContainer
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.linters.files import (
    LocalCodeMigrator,
    FileLoader,
    LocalCodeLinter,
    FolderLoader,
    ImportFileResolver,
    Folder,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver

from tests.unit import locate_site_packages, _samples_path


def test_local_code_migrator_apply_skips_non_existing_file(mock_path_lookup, simple_dependency_resolver) -> None:
    context = create_autospec(LinterContext)
    migrator = LocalCodeMigrator(
        NotebookLoader(),
        FileLoader(),
        FolderLoader(NotebookLoader(), FileLoader()),
        mock_path_lookup,
        CurrentSessionState(),
        simple_dependency_resolver,
        lambda: context,
    )
    path = Path("non_existing_file.py")
    buffer = io.StringIO()

    migrator.apply(MockPrompts({}), path, buffer)

    assert f"{path.as_posix()}:1:0: [path-corrupted] Could not load dependency" in buffer.getvalue()
    context.apply_fixes.assert_not_called()


def test_local_code_migrator_apply_ignores_unsupported_extensions(
    tmp_path, mock_path_lookup, simple_dependency_resolver
) -> None:
    context = create_autospec(LinterContext)
    migrator = LocalCodeMigrator(
        NotebookLoader(),
        FileLoader(),
        FolderLoader(NotebookLoader(), FileLoader()),
        mock_path_lookup,
        CurrentSessionState(),
        simple_dependency_resolver,
        lambda: context,
    )
    buffer = io.StringIO()

    path = tmp_path / "unsupported.ext"
    path.touch()

    migrator.apply(MockPrompts({}), path, buffer)

    assert f"{path.as_posix()}:1:0: [unknown-language] Cannot detect language for" in buffer.getvalue()
    context.apply_fixes.assert_not_called()


def test_linter_context_apply_with_supported_language(tmp_path, mock_path_lookup, simple_dependency_resolver) -> None:
    context = create_autospec(LinterContext)
    context.apply_fixes.return_value = "Hi there!"
    migrator = LocalCodeMigrator(
        NotebookLoader(),
        FileLoader(),
        FolderLoader(NotebookLoader(), FileLoader()),
        mock_path_lookup,
        CurrentSessionState(),
        simple_dependency_resolver,
        lambda: context,
    )

    path = tmp_path / "any.py"
    path.write_text("import tempfile", encoding="utf-8")

    migrator.apply(MockPrompts({}), path)

    assert path.read_text("utf-8") == "Hi there!"


def test_local_code_migrator_apply_walks_directory(tmp_path, mock_path_lookup, simple_dependency_resolver) -> None:
    context = create_autospec(LinterContext)
    context.apply_fixes.return_value = "Hi there!"
    migrator = LocalCodeMigrator(
        NotebookLoader(),
        FileLoader(),
        FolderLoader(NotebookLoader(), FileLoader()),
        mock_path_lookup,
        CurrentSessionState(),
        simple_dependency_resolver,
        lambda: context,
    )

    path = tmp_path / "any.py"
    path.write_text("import tempfile", encoding="utf-8")

    migrator.apply(MockPrompts({}), path.parent)

    assert path.read_text("utf-8") == "Hi there!"


def test_local_code_migrator_fixes_migrated_hive_metastore_table(
    tmp_path, mock_path_lookup, simple_dependency_resolver
) -> None:
    index = TableMigrationIndex([TableMigrationStatus("schema", "table", "catalog", "schema", "table")])
    context = LinterContext(index, CurrentSessionState())
    migrator = LocalCodeMigrator(
        NotebookLoader(),
        FileLoader(),
        FolderLoader(NotebookLoader(), FileLoader()),
        mock_path_lookup,
        CurrentSessionState(),
        simple_dependency_resolver,
        lambda: context,
    )

    path = tmp_path / "read_table.py"
    path.write_text("df = spark.read.table('hive_metastore.schema.table')")

    migrator.apply(MockPrompts({}), path.parent)

    assert "df = spark.read.table('catalog.schema.table')" == path.read_text().rstrip()


@pytest.fixture()
def local_code_linter(mock_path_lookup, migration_index) -> LocalCodeLinter:
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    folder_loader = FolderLoader(notebook_loader, file_loader)
    allow_list = KnownList()
    pip_resolver = PythonLibraryResolver(allow_list)
    session_state = CurrentSessionState()
    import_file_resolver = ImportFileResolver(file_loader, allow_list)
    resolver = DependencyResolver(
        pip_resolver,
        NotebookResolver(NotebookLoader()),
        import_file_resolver,
        import_file_resolver,
        mock_path_lookup,
    )
    return LocalCodeLinter(
        notebook_loader,
        file_loader,
        folder_loader,
        mock_path_lookup,
        session_state,
        resolver,
        lambda: LinterContext(migration_index),
    )


def test_local_code_linter_lint_path_detects_migrated_hive_metastore_table(tmp_path, local_code_linter) -> None:
    path = tmp_path / "read_table.py"
    path.write_text("df = spark.read.table('hive_metastore.old.things')")

    advices = list(local_code_linter.lint_path(path))

    assert len(advices) > 0, "Expect at least one advice"
    assert advices[0] == LocatedAdvice(
        Deprecation(
            code="table-migrated-to-uc-python",
            message="Table hive_metastore.old.things is migrated to brand.new.stuff in Unity Catalog",
            start_line=0,
            start_col=5,
            end_line=0,
            end_col=50,
        ),
        path,
    )


def test_local_code_linter_lint_path_walks_directory(mock_path_lookup, local_code_linter) -> None:
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent.parent / "samples" / "simulate-sys-path"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) > 10
    assert not advices


def test_local_code_linter_lint_path_finds_children_in_context(mock_path_lookup, local_code_linter) -> None:
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent.parent / "samples" / "parent-child-context"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) == 3
    assert advices == [
        LocatedAdvice(
            advice=Advice(
                code='default-format-changed-in-dbr8',
                message='The default format changed in Databricks Runtime 8.0, from Parquet to Delta',
                start_line=3,
                start_col=0,
                end_line=3,
                end_col=33,
            ),
            path=path / "child.py",
        )
    ]


def test_local_code_linter_apply_path_finds_children_in_context(mock_path_lookup, local_code_linter) -> None:
    path = Path(__file__).parent.parent / "samples" / "parent-child-context"

    failures = list(local_code_linter.apply_path(path))

    assert not failures


def test_triple_dot_import() -> None:
    file_resolver = ImportFileResolver(FileLoader(), KnownList())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, "...foo")
    assert not maybe.problems
    assert maybe.dependency is not None
    assert maybe.dependency.path == Path('/some/path/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/../../foo.py'))


def test_single_dot_import() -> None:
    file_resolver = ImportFileResolver(FileLoader(), KnownList())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/to/folder/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, ".foo")
    assert not maybe.problems
    assert maybe.dependency is not None
    assert maybe.dependency.path == Path('/some/path/to/folder/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/foo.py'))


def test_folder_has_repr() -> None:
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    folder = Folder(Path("test"), notebook_loader, file_loader, FolderLoader(notebook_loader, file_loader))
    assert len(repr(folder)) > 0


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
    allow_list = KnownList()
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
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
