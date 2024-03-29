-- viz type=table, name=Jobs, columns=finding,job_id,job_name,creator
-- widget title=Incompatible jobs, row=34, col=0, size_x=8, size_y=12
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    job_id,
    job_name,
    creator
FROM $inventory.jobs
WHERE job_name not like '[UCX]%'
ORDER BY job_id DESC