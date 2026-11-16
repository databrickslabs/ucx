from fastapi import APIRouter

from databricks.labs.ucx.app.routes import assessment, mapping, migration, runs, status

api_router = APIRouter(prefix="/api")
api_router.include_router(status.router)
api_router.include_router(assessment.router)
api_router.include_router(mapping.router)
api_router.include_router(migration.router)
api_router.include_router(runs.router)

__all__ = ["api_router"]
