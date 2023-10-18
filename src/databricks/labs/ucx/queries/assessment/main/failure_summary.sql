-- viz type=table, name=Failure Summary, columns=issue,object_type,issue_count
-- widget title=Failure Summary, col=0, row=65, size_x=6, size_y=8
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