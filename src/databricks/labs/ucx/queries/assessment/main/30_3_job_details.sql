-- viz type=table, name=Job Details, search_by=job_name, columns=job_id,job_name,success,failures,creator
-- widget title=Job Details, row=32, col=0, size_x=8, size_y=8
SELECT
    job_id,
    job_name,
    success,
    failures,
    creator
FROM $inventory.jobs
WHERE job_name not like '[UCX]%'
ORDER BY job_id DESC