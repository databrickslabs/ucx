from databricks.labs.ucx.inventory.types import RequestObjectType


def test_request_object_type():
    assert repr(RequestObjectType) == "<enum 'RequestObjectType'>"
