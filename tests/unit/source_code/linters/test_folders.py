from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import Advisory, CurrentSessionState, LocatedAdvice
from databricks.labs.ucx.source_code.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.folders import FolderLoader
from databricks.labs.ucx.source_code.graph import DependencyResolver, SourceContainer
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.folders import LocalCodeLinter
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from tests.unit import _samples_path


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


def test_local_code_linter_walks_directory(mock_path_lookup, local_code_linter) -> None:
    # TODO remove sample paths and clean up test when the paths is no longer needed
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent / "../samples" / "simulate-sys-path"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) > 10
    assert not advices


def test_local_code_linter_lints_children_in_context(mock_path_lookup, local_code_linter) -> None:
    # TODO remove sample paths and clean up test when the paths is no longer needed
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent.parent / "samples" / "parent-child-context"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) == 3
    assert not advices


def test_local_code_linter_lints_import_from_known_list(tmp_path, mock_path_lookup, local_code_linter) -> None:
    expected_failures = [
        Advisory(
            "jvm-access-in-shared-clusters", "Cannot access Spark Driver JVM on UC Shared Clusters", -1, -1, -1, -1
        ),
        Advisory(
            "legacy-context-in-shared-clusters",
            "sc is not supported on UC Shared Clusters. Rewrite it using spark",
            -1,
            -1,
            -1,
            -1,
        ),
    ]
    ucx_known_url = "https:/github.com/databrickslabs/ucx/blob/main/src/databricks/labs/ucx/source_code/known.json"
    expected_path = Path(f"{ucx_known_url}#pyspark.sql.functions")
    expected_located_advices = [LocatedAdvice(failure, expected_path) for failure in expected_failures]

    content = "import pyspark.sql.functions"  # Has known issues
    path = tmp_path / "file.py"
    path.write_text(content)
    located_advices = list(local_code_linter.lint_path(path))

    assert located_advices == expected_located_advices


def test_local_code_linter_lints_known_s3fs_problems(local_code_linter, mock_path_lookup) -> None:
    known_url = "https://github.com/databrickslabs/ucx/blob/main/src/databricks/labs/ucx/source_code/known.json"
    expected = Advisory(
        "direct-filesystem-access",
        "S3fs library assumes AWS IAM Instance Profile to work with S3, "
        "which is not compatible with Databricks Unity Catalog, that "
        "routes access through Storage Credentials.",
        -1,
        -1,
        -1,
        -1,
    )
    path = mock_path_lookup.resolve(Path("leaf9.py"))
    located_advices = list(local_code_linter.lint_path(path))
    assert located_advices == [LocatedAdvice(expected, Path(known_url + "#s3fs"))]


S3FS_DEPRECATION_MESSAGE = (
    'S3fs library assumes AWS IAM Instance Profile to work with '
    'S3, which is not compatible with Databricks Unity Catalog, '
    'that routes access through Storage Credentials.'
)


@pytest.mark.parametrize(
    "source_code, advice",
    [
        ("import s3fs", Advisory('direct-filesystem-access', S3FS_DEPRECATION_MESSAGE, -1, -1, -1, -1)),
        ("from s3fs import something", Advisory('direct-filesystem-access', S3FS_DEPRECATION_MESSAGE, -1, -1, -1, -1)),
        ("import certifi", None),
        ("from certifi import core", None),
        ("import s3fs, certifi", Advisory('direct-filesystem-access', S3FS_DEPRECATION_MESSAGE, -1, -1, -1, -1)),
        ("from certifi import core, s3fs", None),
        (
            "def func():\n    import s3fs",
            Advisory('direct-filesystem-access', S3FS_DEPRECATION_MESSAGE, -1, -1, -1, -1),
        ),
        ("import s3fs as s", Advisory('direct-filesystem-access', S3FS_DEPRECATION_MESSAGE, -1, -1, -1, -1)),
        (
            "from s3fs.subpackage import something",
            Advisory('direct-filesystem-access', S3FS_DEPRECATION_MESSAGE, -1, -1, -1, -1),
        ),
        ("", None),
    ],
)
def test_local_code_linter_lints_known_s3fs_problems_from_source_code(
    tmp_path,
    mock_path_lookup,
    local_code_linter,
    source_code: str,
    advice: Advisory | None,
) -> None:
    known_url = "https://github.com/databrickslabs/ucx/blob/main/src/databricks/labs/ucx/source_code/known.json"
    module_name = "s3fs.subpackage" if "subpackage" in source_code else "s3fs"
    expected = [LocatedAdvice(advice, Path(f"{known_url}#{module_name}"))] if advice else []
    path = tmp_path / "file.py"
    path.write_text(source_code)
    located_advices = list(local_code_linter.lint_path(path))
    assert located_advices == expected
