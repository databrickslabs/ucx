from fastapi import APIRouter

from databricks.labs.ucx.app.models import Dashboard, RunAssessmentRequest, WorkflowRun
from databricks.labs.ucx.app.routes.dependencies import Service

router = APIRouter(prefix="/assessment", tags=["assessment"])


@router.post("/run")
def run_assessment(service: Service, body: RunAssessmentRequest | None = None) -> WorkflowRun:
    force_refresh = body.force_refresh if body else False
    return service.run_assessment(force_refresh=force_refresh)


@router.get("/dashboard")
def get_assessment_dashboard(service: Service) -> Dashboard:
    return service.assessment_dashboard()
