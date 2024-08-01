/*
--title 'Group Migration Failures'
--height 4
--width 5
--overrides '{"spec":
    {"encodings":{"columns": [
        {"fieldName": "timestamp", "title": "Time of Failure", "booleanValues": ["false", "true"], "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)"},
        {"fieldName": "job_run_id", "title": "Workflow/Task", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}/runs/{{ job_run_id }}", "linkTextTemplate": "{{workflow_name}}/{{task_name}}", "linkTitleTemplate": "Job Run: {{ job_run_id }}", "useMonospaceFont": true},
        {"fieldName": "workspace_group", "title": "Workspace Group", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string"},
        {"fieldName": "account_group", "title": "Account Group", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string"},
        {"fieldName": "error_message", "title": "Error Message", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string"}
    ]}},
    "invisibleColumns": [
        {"fieldName": "job_id", "title": "Workflow", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{workflow_name}}", "linkTitleTemplate": "{{workflow_name}} (ID: {{job_id}})", "useMonospaceFont": true},
        {"fieldName": "workflow_name", "title": "Workflow", "booleanValues": ["false", "true"], "type": "string", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{workflow_name}}", "linkTitleTemplate": "{{workflow_name}} (ID: {{job_id}})", "useMonospaceFont": true},
        {"fieldName": "task_name", "title": "Task", "booleanValues": ["false", "true"], "type": "string", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{workflow_name}}/{{task_name}}", "linkTitleTemplate": "{{workflow_name}} (ID: {{job_id}})", "useMonospaceFont": true}
    ]}'
*/ /*
 * Messages that we're looking for are of the form:
 *   failed-group-migration: {name_in_workspace} -> {name_in_account}: {reason}
 */
SELECT
  CAST(timestamp AS TIMESTAMP) AS timestamp,
  job_run_id,
  REGEXP_EXTRACT(message, '^failed-group-migration: (.+?) -> (.+?): (.+)$', 1) AS workspace_group,
  REGEXP_EXTRACT(message, '^failed-group-migration: (.+?) -> (.+?): (.+)$', 2) AS account_group,
  REGEXP_EXTRACT(message, '^failed-group-migration: (.+?) -> (.+?): (.+)$', 3) AS error_message,
  job_id,
  workflow_name,
  task_name
FROM inventory.logs
WHERE
  workflow_name IN ('migrate-groups', 'migrate-groups-experimental') AND STARTSWITH(message, 'failed-group-migration: ')
ORDER BY
  1 DESC