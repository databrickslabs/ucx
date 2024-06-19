-- --title 'Total Job Count'
SELECT count(*) AS count_total_jobs
FROM inventory.jobs WHERE job_name not like '[UCX]%'
