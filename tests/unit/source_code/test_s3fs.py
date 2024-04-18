from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import Advice, Deprecation, Location
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.dependencies import (
    DependencyLoader,
    SourceContainer,
    DependencyResolver,
)
from databricks.labs.ucx.source_code.dependency_linters import ImportChecker

from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources, _download_side_effect


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            "import s3fs",
            [
                Deprecation(
                    code='dependency-check',
                    message="Deprecated import: --string-- <- s3fs",
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
                Deprecation(
                    code='dependency-check',
                    message="Deprecated import from: --string-- <- s3fs",
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
                Deprecation(
                    code='dependency-check',
                    message="Deprecated import: --string-- <- s3fs",
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
                Deprecation(
                    code='dependency-check',
                    message="Deprecated import: --string-- <- s3fs",
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
                Deprecation(
                    code='dependency-check',
                    message="Deprecated import: --string-- <- s3fs",
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
                Deprecation(
                    code='dependency-check',
                    message="Deprecated import from: --string-- <- s3fs.subpackage",
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
def test_detect_s3fs_import(empty_index, source: str, expected: list[Advice]):
    datas = _load_sources(SourceContainer, "s3fs-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    resolver = DependencyResolver(whitelist)
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = source.encode("utf-8")
    ws.workspace.get_status.return_value = ObjectInfo(path="--string--", object_type=ObjectType.FILE)
    migrator = NotebookMigrator(ws, empty_index, DependencyLoader(ws), resolver)
    object_info = ObjectInfo(path="--string--", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    graph.register_processors(ImportChecker())
    advices = list(graph.process())
    assert advices == expected


@pytest.mark.parametrize(
    " expected",
    (
        [
            Deprecation(
                code='dependency-check',
                # Note test input file names
                message="Deprecated import: root9.py.txt <- leaf9 <- s3fs",
                start_line=1,
                start_col=0,
                end_line=1,
                end_col=11,
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
    migrator = NotebookMigrator(ws, empty_index, DependencyLoader(ws), resolver)
    object_info = ObjectInfo(path="root9.py.txt", object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    graph.register_processors(ImportChecker())
    advices = list(graph.process())
    assert advices == expected


def test_location():
    assert Advice(
        code='dependency-check',
        message="message",
        start_line=1,
        start_col=0,
        end_line=1,
        end_col=11,
    ).as_location() == Location(
        code='dependency-check',
        message="message",
        start_line=1,
        start_col=0,
        end_line=1,
        end_col=11,
    )


def test_invalid_processors(empty_index):
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
    migrator = NotebookMigrator(ws, empty_index, DependencyLoader(ws), resolver)
    object_info = ObjectInfo(path="root9.py.txt", object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    with pytest.raises(ValueError):
        graph.register_processors()
