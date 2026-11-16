from fastapi import APIRouter

from databricks.labs.ucx.app.models import Mapping
from databricks.labs.ucx.app.routes.dependencies import Service

router = APIRouter(prefix="/mapping", tags=["mapping"])


@router.post("")
def create_mapping(service: Service) -> Mapping:
    """Create the initial table mapping from the assessment, overwriting any existing mapping."""
    return service.create_mapping()


@router.get("")
def get_mapping(service: Service) -> Mapping:
    return service.get_mapping()


@router.put("")
def save_mapping(service: Service, mapping: Mapping) -> Mapping:
    return service.save_mapping(mapping)
