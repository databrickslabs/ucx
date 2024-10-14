import io
from unittest.mock import create_autospec

import pytest

from tests.unit import mock_workspace_client

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from databricks.labs.ucx.mixins.cached_workspace_path import WorkspaceCache
from databricks.labs.ucx.source_code.base import guess_encoding


class _WorkspaceCacheFriend(WorkspaceCache):

    @property
    def data_cache(self):
        return self._cache


def test_path_like_returns_cached_instance() -> None:
    cache = _WorkspaceCacheFriend(mock_workspace_client())
    parent = cache.get_workspace_path("/some/path")
    child = parent / "child"
    _cache = getattr(child, "_cache")
    assert _cache == cache.data_cache


def test_non_absolute_path_error() -> None:
    cache = _WorkspaceCacheFriend(mock_workspace_client())
    with pytest.raises(ValueError, match="Invalid workspace path; must be absolute"):
        _ = cache.get_workspace_path("not/an/absolute/path")


def test_iterdir_returns_cached_instances() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    ws.workspace.list.return_value = [ObjectInfo(object_type=ObjectType.FILE, path=s) for s in ("a", "b", "c")]
    cache = _WorkspaceCacheFriend(ws)
    parent = cache.get_workspace_path("/a/dir")
    assert parent.is_dir()
    for child in parent.iterdir():
        _cache = getattr(child, "_cache")
        assert _cache == cache.data_cache


def test_download_is_only_called_once_per_instance() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    cache = WorkspaceCache(ws)
    path = cache.get_workspace_path("/a/path")
    for _ in range(0, 4):
        _ = path.read_text()
    assert ws.workspace.download.call_count == 1


def test_download_is_only_called_once_across_same_resolved_path() -> None:
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    cache = WorkspaceCache(ws)
    cache.get_workspace_path("/a/path").read_text()
    cache.get_workspace_path("/a/b/../path").read_text()
    assert ws.workspace.download.call_count == 1


def test_download_is_only_called_once_across_instances() -> None:
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    cache = WorkspaceCache(ws)
    for _ in range(0, 4):
        path = cache.get_workspace_path("/a/path")
        _ = path.read_text()
    assert ws.workspace.download.call_count == 1


def test_download_is_called_again_after_unlink() -> None:
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    cache = WorkspaceCache(ws)
    path = cache.get_workspace_path("/a/path")
    _ = path.read_text()
    path = cache.get_workspace_path("/a/path")
    path.unlink()
    _ = path.read_text()
    assert ws.workspace.download.call_count == 2


def test_download_is_called_again_after_rename() -> None:
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    cache = WorkspaceCache(ws)
    path = cache.get_workspace_path("/a/path")
    _ = path.read_text()
    path.rename("abcd")
    _ = path.read_text()
    assert ws.workspace.download.call_count == 3  # rename reads the old content


def test_encoding_is_guessed_after_download() -> None:
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    cache = WorkspaceCache(ws)
    path = cache.get_workspace_path("/a/path")
    _ = path.read_text()
    guess_encoding(path)


@pytest.mark.parametrize(
    "mode, data",
    [
        ("r", io.BytesIO("abc".encode("utf-8-sig"))),
        ("rb", io.BytesIO("abc".encode("utf-8-sig"))),
    ],
)
def test_sequential_read_completes(mode: str, data: io.BytesIO) -> None:
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: data
    cache = WorkspaceCache(ws)
    path = cache.get_workspace_path("/a/path")
    with path.open(mode) as file:
        count = 0
        while _ := file.read(1):
            count = count + 1
            if count > 10:
                break
        assert count < 10
