/*
--title 'Code compatability issues'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "code", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "code"},
        {"fieldName": "message", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "message"},
        {"fieldName": "dashboard_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/sql/dashboards/{{ dashboard_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "Dashboard"},
        {"fieldName": "query_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/sql/editor/{{ query_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "Query"}
      ]},
    "invisibleColumns": [
      {"name": "dashboard_parent", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard_parent"},
      {"name": "dashboard_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard_id"},
      {"name": "query_parent", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query_parent"},
      {"name": "query_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query_id"}
    ]
  }}'
*/
SELECT
    object_type,
    data.code,
    data.message,
    data.dashboard_name,
    data.query_name,
    data.dashboard_parent,
    data.query_parent,
    data.query_id
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'QueryProblem'
