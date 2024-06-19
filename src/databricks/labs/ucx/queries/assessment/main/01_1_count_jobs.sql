-- --title 'Total Jobs' --width 2 --height 4
SELECT count(*) AS count_total_jobs
FROM inventory.jobs WHERE job_name not like '[UCX]%'
