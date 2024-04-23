from databricks.labs.ucx.mixins.wspath import WorkspacePath


def test_exists(ws):
    wsp = WorkspacePath(ws, "/Users")
    assert wsp.exists()
