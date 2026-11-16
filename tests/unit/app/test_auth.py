from unittest.mock import patch

import pytest
from databricks.sdk.errors import PermissionDenied

from databricks.labs.ucx.app.auth import AuthMode, app_client, workspace_client
from databricks.labs.ucx.app.context import AppContext


@pytest.fixture(autouse=True)
def fresh_app_client():
    app_client.cache_clear()
    yield
    app_client.cache_clear()


def test_auth_mode_defaults_to_obo() -> None:
    with patch.dict("os.environ", {}, clear=True):
        assert AuthMode.from_env() == AuthMode.OBO


@pytest.mark.parametrize("value, expected", [("obo", AuthMode.OBO), ("SP", AuthMode.SP), (" sp ", AuthMode.SP)])
def test_auth_mode_from_env(value, expected) -> None:
    with patch.dict("os.environ", {"UCX_APP_AUTH_MODE": value}):
        assert AuthMode.from_env() == expected


def test_auth_mode_invalid() -> None:
    with patch.dict("os.environ", {"UCX_APP_AUTH_MODE": "magic"}), pytest.raises(ValueError, match="magic"):
        AuthMode.from_env()


def test_obo_requires_user_token() -> None:
    with pytest.raises(PermissionDenied, match="No user token"):
        workspace_client(AuthMode.OBO, None)


def test_obo_uses_forwarded_token_and_ignores_app_credentials() -> None:
    app_env = {
        "DATABRICKS_HOST": "https://example.cloud.databricks.com",
        "DATABRICKS_CLIENT_ID": "app-sp-id",
        "DATABRICKS_CLIENT_SECRET": "app-sp-secret",
    }
    with patch.dict("os.environ", app_env, clear=True):
        ws = workspace_client(AuthMode.OBO, "user-token")

    assert ws.config.auth_type == "pat"
    assert ws.config.token == "user-token"
    assert ws.config.host == "https://example.cloud.databricks.com"
    assert ws.config.authenticate()["Authorization"] == "Bearer user-token"


def test_sp_uses_app_credentials_from_environment() -> None:
    app_env = {"DATABRICKS_HOST": "https://example.cloud.databricks.com", "DATABRICKS_TOKEN": "sp-token"}
    with patch.dict("os.environ", app_env, clear=True):
        ws = workspace_client(AuthMode.SP, "ignored-user-token")

    assert ws.config.token == "sp-token"


def test_install_folder_pinned_by_environment(ws) -> None:
    with patch.dict("os.environ", {"UCX_INSTALL_FOLDER": "/Applications/ucx"}):
        installation = AppContext(ws).installation

    assert installation.install_folder() == "/Applications/ucx"


def test_app_client_is_shared() -> None:
    app_env = {"DATABRICKS_HOST": "https://example.cloud.databricks.com", "DATABRICKS_TOKEN": "sp-token"}
    with patch.dict("os.environ", app_env, clear=True):
        assert app_client() is app_client()
        assert workspace_client(AuthMode.SP, None) is app_client()
