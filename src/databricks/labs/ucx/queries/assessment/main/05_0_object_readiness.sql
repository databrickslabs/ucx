/* --title 'Readiness' --width 2 --height 6 */
WITH raw AS (
  SELECT
    object_type,
    object_id,
    IF(failures = '[]', 1, 0) AS ready
  FROM inventory.objects
)
SELECT
  object_type,
  CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%') AS readiness
FROM raw
GROUP BY
  object_type
ORDER BY
  readiness DESC