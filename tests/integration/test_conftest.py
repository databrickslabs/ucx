def test_make_ucx_group(make_ucx_group):
    ws, acc = make_ucx_group()
    assert ws is not None
    assert acc is not None


def test_make_ucx_group_with_names(make_ucx_group, make_random):
    ws, acc = make_ucx_group(workspace_group_name=f"foo_{make_random(4)}", account_group_name=f"bar_{make_random(4)}")
    assert ws.display_name != acc.display_name
