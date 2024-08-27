import io
from unittest.mock import create_autospec

from databricks.labs.ucx.blueprint.CachedWorkspacePath import CachedWorkspacePath
from tests.unit import mock_workspace_client

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, ObjectType


def test_path_like_returns_cached_instance():
    parent = CachedWorkspacePath(mock_workspace_client(), "path")
    child = parent / "child"
    assert isinstance(child, CachedWorkspacePath)


def test_iterdir_returns_cached_instances():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(object_type=ObjectType.DIRECTORY)
    ws.workspace.list.return_value = list([ObjectInfo(object_type=ObjectType.FILE, path=s) for s in ["a", "b", "c"]])
    parent = CachedWorkspacePath(ws, "dir")
    assert parent.is_dir()
    for child in parent.iterdir():
        assert isinstance(child, CachedWorkspacePath)


def test_download_is_only_called_once_per_instance():
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    path = CachedWorkspacePath(ws, "path")
    for i in range(0, 4):
        _ = path.read_text()
    assert ws.workspace.download.call_count == 1


def test_download_is_only_called_once_across_instances():
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    for i in range(0, 4):
        path = CachedWorkspacePath(ws, "path")
        _ = path.read_text()
    assert ws.workspace.download.call_count == 1


def test_download_is_called_again_after_unlink():
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    path = CachedWorkspacePath(ws, "path")
    _ = path.read_text()
    path.unlink()
    _ = path.read_text()
    assert ws.workspace.download.call_count == 2


def test_download_is_called_again_after_rename():
    ws = mock_workspace_client()
    ws.workspace.download.side_effect = lambda _, *, format: io.BytesIO("abc".encode())
    path = CachedWorkspacePath(ws, "path")
    _ = path.read_text()
    path.rename("abcd")
    _ = path.read_text()
    assert ws.workspace.download.call_count == 3  # rename reads the old content
