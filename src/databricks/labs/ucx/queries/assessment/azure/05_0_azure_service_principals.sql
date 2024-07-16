/* --title 'Azure Service Principals' --width 6 */
SELECT
  application_id,
  IF(secret_scope = '', 'NA', secret_scope) AS secret_scope,
  IF(secret_key = '', 'NA', secret_key) AS secret_key,
  IF(tenant_id = '', 'NA', tenant_id) AS tenant_id,
  IF(storage_account = '', 'NA', storage_account) AS storage_account
FROM inventory.azure_service_principals