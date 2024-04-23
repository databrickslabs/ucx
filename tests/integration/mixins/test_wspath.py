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


def test_open_text_io(ws, make_random):
    name = make_random()
    wsp = WorkspacePath(ws, f"~/{name}/a/b/c")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_txt = with_user / "hello.txt"
    hello_txt.write_text("Hello, World!")
    assert hello_txt.read_text() == 'Hello, World!'

    files = list(with_user.glob("**/*.txt"))
    assert len(files) == 1
    assert hello_txt == files[0]
    assert files[0].name == 'hello.txt'

    with_user.joinpath("hello.txt").unlink()

    assert not hello_txt.exists()


def test_open_binary_io(ws, make_random):
    name = make_random()
    wsp = WorkspacePath(ws, f"~/{name}")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_bin = with_user.joinpath("hello.bin")
    hello_bin.write_bytes(b"Hello, World!")

    assert hello_bin.read_bytes() == b'Hello, World!'

    with_user.joinpath("hello.bin").unlink()

    assert not hello_bin.exists()


def test_replace(ws, make_random):
    name = make_random()
    wsp = WorkspacePath(ws, f"~/{name}")
    with_user = wsp.expanduser()
    with_user.mkdir(parents=True)

    hello_txt = with_user / "hello.txt"
    hello_txt.write_text("Hello, World!")

    hello_txt.replace(with_user / "hello2.txt")

    assert not hello_txt.exists()
    assert (with_user / "hello2.txt").read_text() == 'Hello, World!'
