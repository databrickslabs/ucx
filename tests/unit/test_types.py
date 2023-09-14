from databricks.labs.ucx.workspace_access.types import RequestObjectType


def test_request_object_type():
    typed = RequestObjectType.AUTHORIZATION
    assert typed == "authorization"
    assert typed.__repr__() == "authorization"
