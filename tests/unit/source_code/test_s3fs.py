from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import (
    DependencyResolver,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.known import AllowList
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver

S3FS_DEPRECATION_MESSAGE = (
    'S3fs library assumes AWS IAM Instance Profile to work with '
    'S3, which is not compatible with Databricks Unity Catalog, '
    'that routes access through Storage Credentials.'
)


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            "import s3fs",
            [
                DependencyProblem(
                    code='direct-filesystem-access',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=11,
                )
            ],
        ),
        (
            "from s3fs import something",
            [
                DependencyProblem(
                    code='direct-filesystem-access',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=26,
                )
            ],
        ),
        ("import certifi", []),
        ("from certifi import core", []),
        (
            "import s3fs, certifi",
            [
                DependencyProblem(
                    code='direct-filesystem-access',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=20,
                )
            ],
        ),
        ("from certifi import core, s3fs", []),
        (
            "def func():\n    import s3fs",
            [
                DependencyProblem(
                    code='direct-filesystem-access',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=1,
                    start_col=4,
                    end_line=1,
                    end_col=15,
                )
            ],
        ),
        (
            "import s3fs as s",
            [
                DependencyProblem(
                    code='direct-filesystem-access',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=16,
                )
            ],
        ),
        (
            "from s3fs.subpackage import something",
            [
                DependencyProblem(
                    code='direct-filesystem-access',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=37,
                )
            ],
        ),
        ("", []),
    ],
)
def test_detect_s3fs_import(empty_index, source: str, expected: list[DependencyProblem], tmp_path, mock_path_lookup):
    sample = tmp_path / "test_detect_s3fs_import.py"
    sample.write_text(source)
    mock_path_lookup.append_path(tmp_path)
    allow_list = AllowList()
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    dependency_resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(sample, CurrentSessionState())
    assert maybe.problems == [_.replace(source_path=sample) for _ in expected]


@pytest.mark.parametrize(
    "expected",
    (
        [
            DependencyProblem(
                code='direct-filesystem-access',
                message='S3fs library assumes AWS IAM Instance Profile to work with '
                'S3, which is not compatible with Databricks Unity Catalog, '
                'that routes access through Storage Credentials.',
                source_path=Path('leaf9.py'),
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=12,
            ),
        ],
    ),
)
def test_detect_s3fs_import_in_dependencies(
    empty_index, expected: list[DependencyProblem], mock_path_lookup, mock_notebook_resolver
):
    file_loader = FileLoader()
    allow_list = AllowList()
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    dependency_resolver = DependencyResolver(pip_resolver, mock_notebook_resolver, import_resolver, mock_path_lookup)
    sample = mock_path_lookup.cwd / "root9.py"
    maybe = dependency_resolver.build_local_file_dependency_graph(sample, CurrentSessionState())
    assert maybe.problems == expected
