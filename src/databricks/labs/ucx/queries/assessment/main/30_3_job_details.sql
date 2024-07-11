-- --title 'Job Details' --filter job_name --width 6
SELECT
    job_id,
    job_name,
    success,
    failures,
    creator
FROM inventory.jobs
WHERE job_name not like '[UCX]%'
ORDER BY job_id DESC
