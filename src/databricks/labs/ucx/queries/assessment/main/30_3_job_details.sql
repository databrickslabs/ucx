/*
--title 'Job Details'
--filter job_name
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "job_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "job_name"},
        {"fieldName": "job_id", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "job_id"},
        {"fieldName": "success", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "success"},
        {"fieldName": "failures", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "failures"},
        {"fieldName": "creator", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "creator"}
      ]}
  }}'
*/
SELECT
  job_name,
  job_id,
  success,
  failures,
  creator
FROM inventory.jobs
WHERE
  NOT job_name LIKE '[UCX]%'
ORDER BY
  job_id DESC
