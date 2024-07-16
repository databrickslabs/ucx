/* --title 'Job Details' --filter job_name --width 6 */
SELECT
  job_id,
  job_name,
  success,
  failures,
  creator
FROM inventory.jobs
WHERE
  NOT job_name LIKE '[UCX]%'
ORDER BY
  job_id DESC