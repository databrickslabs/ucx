import pytest
from databricks.sdk.errors import BadRequest

from databricks.labs.ucx.mixins.wspath import WorkspacePath


def test_exists(ws):
    wsp = WorkspacePath(ws, "/Users")
    assert wsp.exists()


def test_mkdirs(ws, make_random):
    name = make_random()
    wsp = WorkspacePath(ws, f"~/{name}/foo/bar/baz")
    with_user = wsp.expanduser()
    with_user.mkdir()

    wsp_check = WorkspacePath(ws, f"/Users/{ws.current_user.me().user_name}/{name}/foo/bar/baz")
    assert wsp_check.is_dir()

    with pytest.raises(BadRequest):
        wsp_check.parent.rmdir()
    wsp_check.parent.rmdir(recursive=True)

    assert not wsp_check.exists()


def test_open(ws, make_random):
    name = make_random()
    wsp = WorkspacePath(ws, f"~/{name}")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_txt = with_user / "hello.txt"
    hello_txt.write_text("Hello, World!")

    assert b'Hello, World!' == hello_txt.read_bytes()

    with_user.joinpath("hello.txt").unlink()

    assert not hello_txt.exists()

