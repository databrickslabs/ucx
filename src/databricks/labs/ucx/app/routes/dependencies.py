from typing import Annotated

from fastapi import Depends, Request

from databricks.labs.ucx.app.auth import ACCESS_TOKEN_HEADER, AuthMode, app_client, workspace_client
from databricks.labs.ucx.app.context import AppContext
from databricks.labs.ucx.app.service import UcxAppService


def get_service(request: Request) -> UcxAppService:
    """Build a service for the current request.

    A new context is created per request: in on-behalf-of-user mode each request carries a different user's token,
    so nothing that is bound to a workspace client may be shared between requests.
    """
    mode = AuthMode.from_env()
    ws = workspace_client(mode, request.headers.get(ACCESS_TOKEN_HEADER))
    return UcxAppService(AppContext(ws, jobs_ws=app_client()), mode.value)


Service = Annotated[UcxAppService, Depends(get_service)]
