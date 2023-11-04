-- viz type=table, name=Issue Summary, columns=issue,object_type,issue_count
-- widget title=Automatic Upgrade Issue Summary, col=2, row=0, size_x=3, size_y=15
SELECT
  issue,
  object_type,
  COUNT(*) AS issue_count
FROM
  (
    SELECT
      EXPLODE(FROM_JSON(failures, 'array<string>')) AS indv_failure,
      SUBSTRING_INDEX(indv_failure, ":", 1) issue,
      object_type
    from
      $inventory.failure_details
  )
WHERE
  indv_failure IS NOT NULL
  AND indv_failure != ""
GROUP BY
  issue,
  object_type
ORDER BY
  3 DESC;