/*
--title 'Jobs Details'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "job_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/jobs/{{ job_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "job_name"},
        {"fieldName": "creator", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "creator"},
        {"fieldName": "problem", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "problem"}
      ]},
      "invisibleColumns": [
        {"name": "job_id", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "job_id"}
    ]  }}'
*/
SELECT
  job_name,
  job_id,
  creator,
  EXPLODE(
    CASE
        WHEN failures IS NULL OR failures = '[]' THEN ARRAY(NULL)
        ELSE FROM_JSON(failures, 'array<string>')
    END
) AS problem
FROM inventory.jobs
WHERE
  NOT job_name LIKE '[UCX]%'
ORDER BY
  job_id DESC
