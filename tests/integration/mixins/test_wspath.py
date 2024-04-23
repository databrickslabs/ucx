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
    assert wsp_check.exists()