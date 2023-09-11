from databricks.labs.ucx.utils import Request, noop


def test_req():
    req = Request({"test": "test"})
    assert req.as_dict() == {"test": "test"}


def test_noop():
    noop()
    assert True
