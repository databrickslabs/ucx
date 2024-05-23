from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.graph import (
    DependencyResolver,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.whitelist import Whitelist


S3FS_DEPRECATION_MESSAGE = "Use of dependency s3fs is deprecated"


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            "import s3fs",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=11,
                )
            ],
        ),
        (
            "from s3fs import something",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=26,
                )
            ],
        ),
        ("import leeds", []),
        ("from leeds import path", []),
        (
            "import s3fs, leeds",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=18,
                )
            ],
        ),
        ("from leeds import path, s3fs", []),
        (
            "def func():\n    import s3fs",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=2,
                    start_col=4,
                    end_line=2,
                    end_col=15,
                )
            ],
        ),
        (
            "import s3fs as s",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    source_path=Path('path.py'),
                    start_line=1,
                    start_col=0,
                    end_line=1,
                    end_col=16,
                )
            ],
        ),
        (
            "from s3fs.subpackage import something",
            [
                DependencyProblem(
                    code='dependency-check',
                    message='Use of dependency s3fs.subpackage is deprecated',
                    source_path=Path('path.py'),
                    start_line=1,
                    start_col=0,
                    end_line=1,
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
    yml = mock_path_lookup.cwd / "s3fs-python-compatibility-catalog.yml"
    whitelist = Whitelist.parse(yml.read_text(), use_defaults=True)
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(file_loader, whitelist)
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(sample)
    assert maybe.problems == [_.replace(source_path=sample) for _ in expected]


@pytest.mark.parametrize(
    "expected",
    (
        [
            DependencyProblem(
                code='dependency-check',
                message='Use of dependency s3fs is deprecated',
                source_path=Path('leaf9.py'),
                start_line=1,
                start_col=0,
                end_line=1,
                end_col=12,
            ),
        ],
    ),
)
def test_detect_s3fs_import_in_dependencies(
    empty_index, expected: list[DependencyProblem], mock_path_lookup, mock_notebook_resolver
):
    yml = mock_path_lookup.cwd / "s3fs-python-compatibility-catalog.yml"
    file_loader = FileLoader()
    whitelist = Whitelist.parse(yml.read_text(), use_defaults=True)
    import_resolver = ImportFileResolver(file_loader, whitelist)
    dependency_resolver = DependencyResolver([], mock_notebook_resolver, import_resolver, mock_path_lookup)
    sample = mock_path_lookup.cwd / "root9.py"
    maybe = dependency_resolver.build_local_file_dependency_graph(sample)
    assert maybe.problems == expected
