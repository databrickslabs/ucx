from fastapi import APIRouter

from databricks.labs.ucx.app.models import ActiveRun, ActiveRuns
from databricks.labs.ucx.app.routes.dependencies import Service

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("/active")
def get_active_runs(service: Service) -> ActiveRuns:
    """UCX runs in progress, polled by the status window of the UI."""
    return service.active_runs()


@router.get("/{run_id}")
def get_run(service: Service, run_id: int) -> ActiveRun:
    """State of a UCX run, also after it finished. The status window uses this for runs that left the active list."""
    return service.run_status(run_id)
