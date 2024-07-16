/* --title 'UC readiness' --width 2 --height 4 */
WITH raw AS (
  SELECT
    object_type,
    object_id,
    IF(failures = '[]', 1, 0) AS ready
  FROM inventory.objects
)
SELECT
  COALESCE(CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%'), 'N/A') AS readiness
FROM raw