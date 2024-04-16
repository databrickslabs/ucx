from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.dependencies import DependencyLoader, SourceContainer, DependencyResolver
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources, _download_side_effect

S3FS_DEPRECATION_MESSAGE = "Use of dependency s3fs is deprecated"


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            "import s3fs",
            [
                Deprecation(
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
                Deprecation(
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
                Deprecation(
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
                Deprecation(
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
                Deprecation(
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
                Deprecation(
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
def test_detect_s3fs_import(empty_index, source: str, expected: list[Advice]):
    datas = _load_sources(SourceContainer, "s3fs-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    resolver = DependencyResolver(whitelist)
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = source.encode("utf-8")
    ws.workspace.get_status.return_value = ObjectInfo(path="path", object_type=ObjectType.FILE)
    sp = create_autospec(SitePackages)
    migrator = NotebookMigrator(ws, empty_index, DependencyLoader(ws, sp), resolver)
    object_info = ObjectInfo(path="path", language=Language.PYTHON, object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    advices = list(resolver.get_advices())
    assert advices == expected


@pytest.mark.parametrize(
    " expected",
    (
        [
            Deprecation(
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
def test_detect_s3fs_import_in_dependencies(empty_index, expected: list[Advice]):
    paths = ["root9.py.txt", "leaf9.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    datas = _load_sources(SourceContainer, "s3fs-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    resolver = DependencyResolver(whitelist)
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    sp = create_autospec(SitePackages)
    migrator = NotebookMigrator(ws, empty_index, DependencyLoader(ws, sp), resolver)
    object_info = ObjectInfo(path="root9.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    advices = list(resolver.get_advices())
    assert advices == expected
