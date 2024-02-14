-- viz type=table, name=Jobs, columns=failure,job_id,job_name,creator
-- widget title=Incompatible jobs, row=6, col=3, size_x=3, size_y=8
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS failure,
    job_id,
    job_name,
    creator
FROM $inventory.jobs
WHERE job_name not like '[UCX]%'
ORDER BY job_id DESC