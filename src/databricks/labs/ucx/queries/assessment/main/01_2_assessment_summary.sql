<<<<<<< HEAD:src/databricks/labs/ucx/queries/assessment/main/01_2_assessment_summary.sql
-- viz type=table, name=Assessment Summary, search_by=failure, columns=failure,count
-- widget title=Assessment Summary, row=1, col=2, size_x=4, size_y=8
WITH raw AS (
  SELECT EXPLODE(FROM_JSON(failures, 'array<string>')) AS failure FROM $inventory.objects WHERE failures <> '[]'
)
SELECT failure as `item`, COUNT(*) AS count FROM raw GROUP BY failure
ORDER BY count DESC, failure DESC
