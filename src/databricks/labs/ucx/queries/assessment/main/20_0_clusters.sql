/*
--title 'Incompatible clusters'
--width 3
--height 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "cluster_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "/compute/clusters/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "cluster_id"},
        {"fieldName": "cluster_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/compute/clusters/{{ cluster_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link",  "title": "cluster_name"},
        {"fieldName": "finding", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "finding"},
        {"fieldName": "creator", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "creator"}
      ]}
  }}'
*/
SELECT
  cluster_id,
  cluster_name,
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  creator
FROM inventory.clusters
WHERE
  NOT STARTSWITH(cluster_name, 'job-')
ORDER BY
  cluster_id DESC
