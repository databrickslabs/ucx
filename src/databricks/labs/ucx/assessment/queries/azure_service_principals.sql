select
  application_id,
  secret_scope,
  secret_key,
  tenant_id,
  storage_account
from
  $inventory.azure_service_principals