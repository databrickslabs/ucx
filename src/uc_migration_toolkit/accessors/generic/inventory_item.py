from pydantic import BaseModel


class PermissionsInventoryItem(BaseModel):
    object_id: str
    object_type: str
    object_info: dict
    object_permissions: dict
