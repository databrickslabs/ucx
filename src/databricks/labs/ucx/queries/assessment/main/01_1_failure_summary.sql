-- viz type=table, name=Failure Summary, search_by=failure, columns=failure,count
-- widget title=Failure Summary, row=1, col=1, size_x=2, size_y=8
WITH raw AS (
  SELECT EXPLODE(FROM_JSON(failures, 'array<string>')) AS failure FROM $inventory.objects WHERE failures <> '[]'
)
SELECT failure, COUNT(*) AS count FROM raw GROUP BY failure
ORDER BY count DESC, failure DESC