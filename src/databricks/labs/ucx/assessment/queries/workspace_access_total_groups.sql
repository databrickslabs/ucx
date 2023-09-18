-- UCX: type: table
SELECT
  object_type,
  COUNT(DISTINCT object_id) groups
FROM $inventory.permissions
GROUP BY 1