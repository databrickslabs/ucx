import base64
import json

import pytest
from databricks.sdk.oauth import Token


@pytest.fixture
def az_token(mocker):
    token = json.dumps({"aud": "foo", "tid": "bar"}).encode("utf-8")
    str_token = base64.b64encode(token).decode("utf-8").replace("=", "")
    tok = Token(access_token=f"header.{str_token}.sig")
    mocker.patch("databricks.sdk.oauth.Refreshable.token", return_value=tok)
