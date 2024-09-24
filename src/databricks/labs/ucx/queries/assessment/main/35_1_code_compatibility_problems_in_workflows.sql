/*
--title 'Workflow migration problems'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "path", "booleanValues": ["false", "true"], "linkUrlTemplate": "/#workspace/{{ link }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "path"},
        {"fieldName": "code", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "code"},
        {"fieldName": "message", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "message"},
        {"fieldName": "workflow_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/jobs/{{ workflow_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "workflow_name"},
        {"fieldName": "task_key", "booleanValues": ["false", "true"], "imageUrlTemplate": "{{ @ }}", "linkUrlTemplate": "/jobs/{{ workflow_id }}/tasks/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "task_key"},
        {"fieldName": "start_line", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "start_line"},
        {"fieldName": "start_col", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "start_col"},
        {"fieldName": "end_line", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "end_line"},
        {"fieldName": "end_col", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "end_col"}
      ]},
    "invisibleColumns": [
      {"name": "link", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "link"},
      {"name": "workflow_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "workflow_id"}
    ]
  }}'
*/
SELECT
  substring_index(path, '@databricks.com/', -1) as path,
  path as link,
  code,
  message,
  job_id AS workflow_id,
  job_name AS workflow_name,
  task_key,
  start_line,
  start_col,
  end_line,
  end_col
FROM inventory.workflow_problems
