-- --title 'Azure Service Principals'
-- widget title=Azure Service Principals, row=0, col=0, size_x=3, size_y=8
select
  application_id,
  if(secret_scope = '', "NA", secret_scope) secret_scope,
  if(secret_key = '', "NA", secret_key) secret_key,
  if(tenant_id = '', "NA", tenant_id) tenant_id,
  if(storage_account = '', "NA", storage_account) storage_account
from
  inventory.azure_service_principals
