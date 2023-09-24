-- viz type=table, name=Jobs, columns=job_id,job_name,creator,compatible,failures
-- widget title=Jobs, col=3, row=10, size_x=3, size_y=4
SELECT job_id,
       job_name,
       creator,
       CASE
           WHEN success=1 THEN "Compatible"
           ELSE "Incompatible"
       END AS compatible,
       failures
FROM $inventory.jobs
WHERE job_name not like '[UCX]%'
