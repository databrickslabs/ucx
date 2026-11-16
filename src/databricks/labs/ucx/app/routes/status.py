from fastapi import APIRouter

from databricks.labs.ucx.app.models import Status
from databricks.labs.ucx.app.routes.dependencies import Service

router = APIRouter(tags=["status"])


@router.get("/status")
def get_status(service: Service) -> Status:
    return service.status()
