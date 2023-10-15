-- viz type=table, name=Failure Summary, columns=issue,object_type,issue_count
-- widget title=Failure Summary, col=0, row=65, size_x=6, size_y=8
SELECT
  issue,
  object_type,
  issue_count
FROM
  $inventory.vw_failure_summary;