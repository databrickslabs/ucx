"""Authentication for the UCX app.

Two modes are supported, selected with the ``UCX_APP_AUTH_MODE`` environment variable:

- ``obo`` (default): on-behalf-of the logged-in user, for everything that user authorization supports. Databricks
  Apps forwards the user's token in the ``X-Forwarded-Access-Token`` header. The UCX installation (workspace files),
  the inventory (SQL) and the Unity Catalog checks are then accessed with the user's own permissions, like the CLI.
  User authorization has no scope for the Jobs API, so starting and watching UCX workflows always uses the app's
  service principal, which `deploy-app` grants run permissions on the UCX jobs.
- ``sp``: the app's own service principal for everything.
"""

import os
from enum import Enum
from functools import cache

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied

from databricks.labs.ucx.__about__ import __version__

AUTH_MODE_ENV = "UCX_APP_AUTH_MODE"
ACCESS_TOKEN_HEADER = "X-Forwarded-Access-Token"
PRODUCT_NAME = "ucx-app"


class AuthMode(str, Enum):
    OBO = "obo"
    SP = "sp"

    @classmethod
    def from_env(cls) -> "AuthMode":
        value = os.environ.get(AUTH_MODE_ENV, cls.OBO.value).strip().lower()
        try:
            return cls(value)
        except ValueError:
            raise ValueError(f"Invalid {AUTH_MODE_ENV}={value!r}, expected one of: obo, sp") from None


@cache
def app_client() -> WorkspaceClient:
    """The app's service principal. Shared by all requests: it doesn't depend on the user."""
    return WorkspaceClient(product=PRODUCT_NAME, product_version=__version__)


def workspace_client(mode: AuthMode, user_token: str | None) -> WorkspaceClient:
    """Create the workspace client for a request, according to the auth mode. Not used for the Jobs API."""
    if mode == AuthMode.SP:
        return app_client()
    if not user_token:
        raise PermissionDenied(
            "No user token forwarded to the app. Enable user authorization for the app, "
            f"or deploy it with {AUTH_MODE_ENV}=sp."
        )
    # Databricks Apps also injects the app's service principal credentials into the environment. Pinning the
    # auth type makes the SDK use only the forwarded user token and ignore those credentials.
    return WorkspaceClient(
        host=os.environ.get("DATABRICKS_HOST"),
        token=user_token,
        auth_type="pat",
        product=PRODUCT_NAME,
        product_version=__version__,
    )
