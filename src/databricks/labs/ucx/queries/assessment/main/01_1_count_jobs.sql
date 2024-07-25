/* --title 'Total Jobs' --height 4 */
SELECT
  COUNT(*) AS count_total_jobs
FROM inventory.jobs
WHERE
  NOT job_name LIKE '[UCX]%'