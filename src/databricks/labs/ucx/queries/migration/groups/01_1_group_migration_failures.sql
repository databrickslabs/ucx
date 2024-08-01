/*
--title 'Group Migration Failures'
--height 4
--width 5
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "timestamp", "title": "Time of Failure", "booleanValues": ["false", "true"], "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)"},
        {"fieldName": "task_name", "title": "Workflow/Task", "booleanValues": ["false", "true"], "type": "string", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{ workflow_name }}/{{ @ }}", "linkTitleTemplate": "{{ workflow_name }} (ID: {{ job_id }})", "linkOpenInNewTab": true, "useMonospaceFont": true},
        {"fieldName": "job_run_id", "title": "Run ID", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}/runs/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "Job Run: {{ @ }}", "linkOpenInNewTab": true},
        {"fieldName": "workspace_group", "title": "Workspace Group", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string"},
        {"fieldName": "account_group", "title": "Account Group", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string"},
        {"fieldName": "error_message", "title": "Error Message", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string"}
      ]},
    "invisibleColumns": [
      {"name": "job_id", "title": "Workflow", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{ workflow_name }}", "linkTitleTemplate": "{{ workflow_name }} (ID: {{ job_id }})", "useMonospaceFont": true},
      {"name": "workflow_name", "title": "Workflow", "booleanValues": ["false", "true"], "type": "string", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{ workflow_name }}", "linkTitleTemplate": "{{ workflow_name }} (ID: {{ job_id }})", "useMonospaceFont": true}
    ]
   }}'
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
    workflow_name IN ('migrate-groups', 'migrate-groups-experimental')
), migration_failures AS (
  /* Messages that we're looking for are of the form:
   *   failed-group-migration: {name_in_workspace} -> {name_in_account}: {reason}
   */
  SELECT
    timestamp,
    REGEXP_EXTRACT(message, '^failed-group-migration: (.+?) -> (.+?): (.+)$', 1) AS workspace_group,
    REGEXP_EXTRACT(message, '^failed-group-migration: (.+?) -> (.+?): (.+)$', 2) AS account_group,
    REGEXP_EXTRACT(message, '^failed-group-migration: (.+?) -> (.+?): (.+)$', 3) AS error_message,
    job_run_id,
    job_id,
    workflow_name,
    task_name
  FROM logs_latest_job_runs
  WHERE
    STARTSWITH(message, 'failed-group-migration: ')
)
SELECT
  timestamp,
  workspace_group,
  account_group,
  error_message,
  job_run_id,
  job_id,
  workflow_name,
  task_name
FROM migration_failures
ORDER BY
  1