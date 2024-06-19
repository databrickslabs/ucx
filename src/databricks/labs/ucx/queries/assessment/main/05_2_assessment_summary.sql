-- --title 'Assessment Summary'
WITH raw AS (
  SELECT EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding
  FROM inventory.objects WHERE failures <> '[]'
)
SELECT finding as `finding`, COUNT(*) AS count
FROM raw
GROUP BY finding
ORDER BY count DESC, finding DESC
