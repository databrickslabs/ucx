-- viz type=table, name=Assessment Summary, search_by=finding, columns=finding,count
-- widget title=Assessment Summary, row=7, col=2, size_x=4, size_y=8
WITH raw AS (
  SELECT EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding
  FROM $inventory.objects WHERE failures <> '[]'
)
SELECT finding as `finding`, COUNT(*) AS count 
FROM raw 
GROUP BY finding
ORDER BY count DESC, finding DESC
