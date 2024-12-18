/*
--title 'Query compatability issues'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "workspace_id", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "workspace_id"},
        {"fieldName": "code", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "code"},
        {"fieldName": "message", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "message"},
        {"fieldName": "dashboard_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/sql/dashboards/{{ dashboard_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard"},
        {"fieldName": "query_name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/sql/editor/{{ query_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query"}
      ]},
    "invisibleColumns": [
      {"name": "dashboard_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "dashboard_id"},
      {"name": "query_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "query_id"}
    ]
  }}'
*/
SELECT
    workspace_id,
    data.code,
    data.message,
    data.dashboard_name,
    data.query_name,
    -- Below are invisible columns used in links url templates
    data.dashboard_id,
    data.query_id
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'QueryProblem'
