-- viz type=table, name=Azure SPNs, columns=application_id,secret_scope,secret_key,tenant_id,storage_account
-- widget title=Azure SPNs, col=0, row=49, size_x=6, size_y=8
select
  application_id,
  if(secret_scope = '', "NA", secret_scope) secret_scope,
  if(secret_key = '', "NA", secret_key) secret_key,
  if(tenant_id = '', "NA", tenant_id) tenant_id,
  if(storage_account = '', "NA", storage_account) storage_account
from
  $inventory.azure_service_principals