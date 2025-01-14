import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.source_code.base import CurrentSessionState, LocatedAdvice, Advice
from databricks.labs.ucx.source_code.graph import DependencyResolver, SourceContainer
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import KnownList

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.linters.files import (
    LocalCodeMigrator,
    FileLoader,
    LocalCodeLinter,
    FolderLoader,
    ImportFileResolver,
    Folder,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from tests.unit import locate_site_packages, _samples_path


def test_local_code_migrator_apply_skips_non_existing_file(caplog) -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = LocalCodeMigrator(lambda: languages)
    path = Path("non_existing_file.py")
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.linters.files"):
        assert not migrator.apply(path)
    assert f"Skip non-existing file: {path}" in caplog.messages


def test_local_code_migrator_apply_ignores_unsupported_extensions(caplog, tmp_path) -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = LocalCodeMigrator(lambda: languages)
    path = tmp_path / "unsupported.ext"
    path.touch()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.linters.files"):
        assert not migrator.apply(path)
    assert f"Skip fixing file with unsupported extension: {path}" in caplog.messages


def test_local_code_migrator_apply_ignores_unsupported_language_on_context(tmp_path, caplog) -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = LocalCodeMigrator(lambda: languages)
    migrator._extensions[".py"] = Language.R  # pylint: disable=protected-access
    path = tmp_path / 'unsupported.py'
    path.touch()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.linters.files"):
        assert not migrator.apply(path)
    assert "Skip fixing unsupported language: R" in caplog.messages


def test_linter_context_apply_with_supported_language(tmp_path) -> None:
    path = tmp_path / "any.py"
    path.write_text("import tempfile", encoding="utf-8")
    context = create_autospec(LinterContext)
    context.apply_fixes.return_value = "Hi there!"
    migrator = LocalCodeMigrator(lambda: context)

    migrator.apply(path)

    assert path.read_text("utf-8") == "Hi there!"


def test_local_code_migrator_apply_walks_directory(tmp_path) -> None:
    path = tmp_path / "any.py"
    path.write_text("import tempfile", encoding="utf-8")
    context = create_autospec(LinterContext)
    context.apply_fixes.return_value = "Hi there!"
    migrator = LocalCodeMigrator(lambda: context)

    migrator.apply(path.parent)

    assert path.read_text("utf-8") == "Hi there!"


@pytest.fixture()
def local_code_linter(mock_path_lookup, migration_index):
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


def test_linter_walks_directory(mock_path_lookup, local_code_linter) -> None:
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent / "../samples" / "simulate-sys-path"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) > 10
    assert not advices


def test_linter_lints_children_in_context(mock_path_lookup, local_code_linter) -> None:
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
