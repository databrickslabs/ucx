-- --title 'Table estimates'
SELECT
  table_name,
  object_type,
  table_format,
  estimated_hours
FROM inventory.table_estimates
