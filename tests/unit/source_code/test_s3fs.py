from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.dependencies import (
    SourceContainer,
    DependencyResolver,
    LocalFileLoader,
    DependencyGraphBuilder,
    LocalNotebookLoader,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources, _load_dependency_side_effect, locate_site_packages


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
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            ],
        ),
        (
            "from s3fs import something",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
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
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            ],
        ),
        (
            "import s3fs as s",
            [
                DependencyProblem(
                    code='dependency-check',
                    message=S3FS_DEPRECATION_MESSAGE,
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            ],
        ),
        (
            "from s3fs.subpackage import something",
            [
                DependencyProblem(
                    code='dependency-check',
                    message='Use of dependency s3fs.subpackage is deprecated',
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            ],
        ),
        ("", []),
    ],
)
def test_detect_s3fs_import(empty_index, source: str, expected: list[DependencyProblem]):
    datas = _load_sources(SourceContainer, "s3fs-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    sources = {"path": source}
    file_loader = create_autospec(LocalFileLoader)
    file_loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    file_loader.is_file.return_value = True
    file_loader.is_notebook.return_value = False
    site_packages = SitePackages.parse(locate_site_packages())
    resolver = DependencyResolver(whitelist, site_packages, file_loader, LocalNotebookLoader())
    builder = DependencyGraphBuilder(resolver)
    builder.build_local_file_dependency_graph(Path("path"))
    problems: list[DependencyProblem] = resolver.problems
    assert problems == expected


@pytest.mark.parametrize(
    " expected",
    (
        [
            DependencyProblem(
                code='dependency-check',
                message='Use of dependency s3fs is deprecated',
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=0,
            ),
        ],
    ),
)
def test_detect_s3fs_import_in_dependencies(empty_index, expected: list[DependencyProblem]):
    paths = ["root9.py.txt", "leaf9.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "s3fs-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    file_loader = create_autospec(LocalFileLoader)
    file_loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    file_loader.is_file.return_value = True
    file_loader.is_notebook.return_value = False
    site_packages = SitePackages.parse(locate_site_packages())
    resolver = DependencyResolver(whitelist, site_packages, file_loader, LocalNotebookLoader())
    builder = DependencyGraphBuilder(resolver)
    builder.build_local_file_dependency_graph(Path("root9.py.txt"))
    problems: list[DependencyProblem] = resolver.problems
    assert problems == expected
