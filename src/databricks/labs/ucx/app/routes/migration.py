from fastapi import APIRouter

from databricks.labs.ucx.app.models import MigrationPrecheck, MigrationRun, RunMigrationRequest
from databricks.labs.ucx.app.routes.dependencies import Service

router = APIRouter(prefix="/migration", tags=["migration"])


@router.get("/precheck")
def migration_precheck(service: Service) -> MigrationPrecheck:
    return service.migration_precheck()


@router.post("/run")
def run_migration(service: Service, body: RunMigrationRequest | None = None) -> MigrationRun:
    """Run the selected migration workflows sequentially. Defaults to only `migrate-tables`."""
    return service.run_migration(body or RunMigrationRequest())
