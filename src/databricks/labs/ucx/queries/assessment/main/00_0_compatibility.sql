-- viz type=counter, --title Workspace UC readiness, counter_label=UC readiness, value_column=readiness
-- widget row=1, col=0, size_x=1, size_y=3
WITH raw AS (
  SELECT object_type, object_id, IF(failures == '[]', 1, 0) AS ready
  FROM inventory.objects
)
SELECT COALESCE(CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%'), 'N/A') AS readiness FROM raw
