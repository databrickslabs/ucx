-- viz type=table, name=Assessment warning and error messages, columns=timestamp,job_id,workflow_name,task_name,job_run_id,level,component,message
-- widget title=Assessment warning and error messages, row=44, col=0, size_x=8, size_y=12
SELECT *
FROM $inventory.logs
WHERE job_run_id = (SELECT MAX(job_run_id) FROM $inventory.logs)
ORDER BY timestamp ASC