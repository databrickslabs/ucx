-- viz type=table, name=Azure SPNs, columns=application_id,secret_scope,secret_key,tenant_id,storage_account
-- widget title=Azure SPNs, col=0, row=41, size_x=6, size_y=8
select
  application_id,
  secret_scope,
  secret_key,
  tenant_id,
  storage_account
from
  $inventory.azure_service_principals