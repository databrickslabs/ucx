/* --title 'UC readiness' --height 3 */
SELECT
  COALESCE(CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%'), 'N/A') AS readiness
FROM (
  SELECT
    object_type,
    object_id,
    IF(failures = '[]', 1, 0) AS ready
  FROM inventory.objects
)