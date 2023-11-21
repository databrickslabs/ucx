def test_make_ucx_group(make_ucx_group):
    ws_group_a, acc_group_a = make_ucx_group()
    assert ws_group_a is not None
    assert acc_group_a is not None
