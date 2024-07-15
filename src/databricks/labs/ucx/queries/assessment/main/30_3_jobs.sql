/* --title 'Incompatible Jobs' --width 6 */
SELECT
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  job_id,
  job_name,
  creator
FROM inventory.jobs
WHERE
  NOT job_name LIKE '[UCX]%'
ORDER BY
  job_id DESC