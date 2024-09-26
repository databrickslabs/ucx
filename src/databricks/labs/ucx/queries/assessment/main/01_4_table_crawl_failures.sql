/*
--title 'Table Crawl Failures'
--height 4
--width 4
*/
WITH latest_job_runs AS (
  SELECT
    timestamp,
    job_id,
    job_run_id
  FROM (
    SELECT
      CAST(timestamp AS TIMESTAMP) AS timestamp,
      job_id,
      job_run_id,
      ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY CAST(timestamp AS TIMESTAMP) DESC) = 1 AS latest_run_of_job
    FROM inventory.logs
  )
  WHERE
    latest_run_of_job
), logs_latest_job_runs AS (
  SELECT
    CAST(logs.timestamp AS TIMESTAMP) AS timestamp,
    message,
    job_run_id,
    job_id,
    workflow_name,
    task_name
  FROM inventory.logs
  JOIN latest_job_runs
    USING (job_id, job_run_id)
  WHERE
    workflow_name IN ('assessment')
), table_crawl_failures AS (
SELECT
    timestamp,
    REGEXP_EXTRACT(message, '^failed-table-crawl: (.+?) -> (.+?): (.+)$', 1) AS error_reason,
    REGEXP_EXTRACT(message, '^failed-table-crawl: (.+?) -> (.+?): (.+)$', 2) AS error_entity,
    REGEXP_EXTRACT(message, '^failed-table-crawl: (.+?) -> (.+?): (.+)$', 3) AS error_message,
    job_run_id,
    job_id,
    workflow_name,
    task_name
FROM logs_latest_job_runs
  WHERE
    STARTSWITH(message, 'failed-table-crawl: ')
)
SELECT
  timestamp,
  error_reason,
  error_entity,
  error_message,
  job_run_id,
  job_id,
  workflow_name,
  task_name
FROM table_crawl_failures
ORDER BY
  1
