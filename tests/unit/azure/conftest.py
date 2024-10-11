import base64
import json
from unittest.mock import create_autospec

import pytest
from databricks.sdk.oauth import Token

from databricks.labs.ucx.azure.resources import AzureAPIClient
from . import get_az_api_mapping


@pytest.fixture
def azure_api_client():
    token = json.dumps({"aud": "foo", "tid": "bar"}).encode("utf-8")
    str_token = base64.b64encode(token).decode("utf-8").replace("=", "")
    tok = Token(access_token=f"header.{str_token}.sig")
    api_client = create_autospec(AzureAPIClient)
    api_client.token.return_value = tok
    for method_name in "get", "put", "post":
        method = getattr(api_client, method_name)
        method.side_effect = get_az_api_mapping
        # Set attributes required for `functools.wraps`
        for attr in "__name__", "__qualname__", "__annotations__", "__type_params__":
            setattr(method, attr, getattr(get_az_api_mapping, attr, None))
    return api_client
