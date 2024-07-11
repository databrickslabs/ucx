-- --title 'Warning messages' --width 6
SELECT
  from_unixtime(timestamp) AS timestamp,
  job_id,
  workflow_name,
  task_name,
  job_run_id,
  level,
  substring(component, length('databricks.labs.') + 1) AS component,  -- left strip 'databricks.labs.'
  message
FROM inventory.logs
WHERE job_run_id = (
    SELECT DISTINCT job_run_id FROM inventory.logs WHERE timestamp = (SELECT MAX(timestamp) FROM inventory.logs)
)
ORDER BY timestamp ASC
