-- viz type=table, name=Object Type Readiness, columns=object_type,readiness
-- widget title=Readiness, row=7, col=0, size_x=2, size_y=8
WITH raw AS (
  SELECT object_type, object_id, IF(failures == '[]', 1, 0) AS ready 
  FROM $inventory.objects
)
SELECT object_type, CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%') AS readiness
FROM raw
GROUP BY object_type
ORDER BY readiness DESC