-- --title 'Jobs'
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    job_id,
    job_name,
    creator
FROM inventory.jobs
WHERE job_name not like '[UCX]%'
ORDER BY job_id DESC
