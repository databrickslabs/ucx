from databricks.labs.ucx.utils import Request


def test_req():
    req = Request({"test": "test"})
    assert req.as_dict() == {"test": "test"}
