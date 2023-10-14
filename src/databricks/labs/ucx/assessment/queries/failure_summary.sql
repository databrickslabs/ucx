-- viz type=table, name=Failure Summary, columns=issue,object_type,issue_count,jobs_issue_percentage,clusters_issue_percentage,gis_issue_percentage,pipelines_issue_percentage
-- widget title = Failure Summary, col=0, row=65, size_x=6, size_y=8
SELECT
  issue,
  object_type,
  issue_count,
  jobs_issue_percentage,
  clusters_issue_percentage,
  gis_issue_percentage,
  pipelines_issue_percentage
FROM
  $inventory.vw_failure_summary;