/* --title 'Total Jobs' --width 1 --height 3 */
SELECT
  COUNT(*) AS count_total_jobs
FROM inventory.jobs
WHERE
  NOT job_name LIKE '[UCX]%'