"""Entry point of the UCX Databricks App: ``gunicorn databricks.labs.ucx.app.main:app``."""

import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk.errors import DatabricksError, NotFound, PermissionDenied

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.app.routes import api_router
from databricks.labs.ucx.app.service import MappingValidationError, ResourceNotInstalled

logger = logging.getLogger(__name__)

# The frontend static export is built into the wheel at this location, see `make app-ui`.
STATIC_DIR = Path(__file__).parent / "static"


def _error(status_code: int):
    def handler(_: Request, exc: Exception) -> JSONResponse:
        if status_code >= 500:
            logger.error("Request failed", exc_info=exc)
        return JSONResponse(status_code=status_code, content={"detail": str(exc)})

    return handler


def create_app(static_dir: Path = STATIC_DIR) -> FastAPI:
    fastapi_app = FastAPI(title="UCX", version=__version__, docs_url="/api/docs", openapi_url="/api/openapi.json")
    fastapi_app.include_router(api_router)
    # Handlers are looked up along the exception's class hierarchy, so the most specific one wins.
    fastapi_app.add_exception_handler(MappingValidationError, _error(422))
    fastapi_app.add_exception_handler(ValueError, _error(400))  # e.g. "No tables found. Please run the assessment."
    fastapi_app.add_exception_handler(RuntimeWarning, _error(409))  # unmet migration prerequisites
    fastapi_app.add_exception_handler(ResourceNotInstalled, _error(404))
    fastapi_app.add_exception_handler(NotFound, _error(404))  # e.g. UCX is not installed for this user
    fastapi_app.add_exception_handler(PermissionDenied, _error(403))
    fastapi_app.add_exception_handler(DatabricksError, _error(502))
    if static_dir.is_dir():
        fastapi_app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    else:
        logger.warning(f"Frontend not found at {static_dir}, serving the API only")
    return fastapi_app


app = create_app()
