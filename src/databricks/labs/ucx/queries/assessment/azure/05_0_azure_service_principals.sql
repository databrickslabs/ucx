-- --title 'Azure Service Principals'
select
  application_id,
  if(secret_scope = '', "NA", secret_scope) secret_scope,
  if(secret_key = '', "NA", secret_key) secret_key,
  if(tenant_id = '', "NA", tenant_id) tenant_id,
  if(storage_account = '', "NA", storage_account) storage_account
from
  inventory.azure_service_principals
