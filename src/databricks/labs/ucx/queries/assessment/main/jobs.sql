-- viz type=table, name=Jobs, columns=job_id,job_name,creator,compatible,failures
-- widget title=Jobs, col=0, row=45, size_x=6, size_y=8
SELECT job_id,
       job_name,
       creator,
       IF(success=1, "Compatible", "Incompatible") AS compatible,
       failures
FROM $inventory.jobs
WHERE job_name not like '[UCX]%'
