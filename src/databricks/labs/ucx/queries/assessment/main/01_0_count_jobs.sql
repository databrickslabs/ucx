-- --title 'Total Job Count'
-- widget row=2, col=0, size_x=2, size_y=5
SELECT count(*) AS count_total_jobs
FROM inventory.jobs WHERE job_name not like '[UCX]%'
