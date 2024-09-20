/*
--title 'Dashboard migration problems'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "code", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "code"},
        {"fieldName": "message", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "message"},
        {"fieldName": "dashboard_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/dashboards/{{ dashboard_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard_name"},
        {"fieldName": "query_name", "booleanValues": ["false", "true"], "imageUrlTemplate": "{{ @ }}", "linkUrlTemplate": "/queries/{{ query_id }}/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query_name"}
      ]},
    "invisibleColumns": [
      {"name": "dashboard_parent", "booleanValues": ["false", "true"], "linkUrlTemplate": "/dashboards/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard_parent"},
      {"name": "dashboard_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "/dashboards/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard_id"},
      {"name": "query_parent", "booleanValues": ["false", "true"], "linkUrlTemplate": "/dashboards/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query_parent"},
      {"name": "query_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "/dashboards/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query_id"}
    ]
  }}'
*/
SELECT
  dashboard_id,
  dashboard_parent,
  dashboard_name,
  query_id,
  query_parent,
  query_name,
  code,
  message
FROM inventory.query_problems
