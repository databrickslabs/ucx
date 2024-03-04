import base64
import json
import pathlib
from unittest import mock
from unittest.mock import create_autospec

from databricks.sdk.oauth import Token

from databricks.labs.ucx.azure.resources import AzureAPIClient

__dir = pathlib.Path(__file__).parent


def _load_fixture(filename: str):
    with (__dir / filename).open("r") as f:
        return json.load(f)


def get_az_api_mapping(*args, **_):
    mapping = _load_fixture("azure/mappings.json")[0]
    if args[0] in mapping:
        return mapping[args[0]]
    if args[1] in mapping:
        return mapping[args[1]]
    return {}


def azure_api_client():
    token = json.dumps({"aud": "foo", "tid": "bar"}).encode("utf-8")
    str_token = base64.b64encode(token).decode("utf-8").replace("=", "")
    tok = Token(access_token=f"header.{str_token}.sig")
    api_client = create_autospec(AzureAPIClient)
    type(api_client).token = mock.PropertyMock(return_value=tok)
    api_client.get.side_effect = get_az_api_mapping
    api_client.put.side_effect = get_az_api_mapping
    api_client.post.side_effect = get_az_api_mapping
    return api_client
