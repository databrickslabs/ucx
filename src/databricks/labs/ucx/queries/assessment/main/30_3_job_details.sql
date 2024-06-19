-- --title Job Details, search_by=job_name, columns=job_id,job_name,success,failures,creator
SELECT
    job_id,
    job_name,
    success,
    failures,
    creator
FROM inventory.jobs
WHERE job_name not like '[UCX]%'
ORDER BY job_id DESC
