/*
--title 'Dashboards pending migration'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "workspace_id", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "workspace_id"},
        {"fieldName": "owner", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "owner"},
        {"fieldName": "name", "title": "Name", "type": "string", "displayAs": "link", "linkUrlTemplate": "{{ dashboard_link }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "booleanValues": ["false", "true"]},
        {"fieldName": "dashboard_type", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "dashboard_type"},
        {"fieldName": "failure", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "failure"}
      ]},
    "invisibleColumns": [
      {"fieldName": "dashboard_link", "title": "dashboard_link", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]}
    ]
  }}'
*/
SELECT
  workspace_id,
  owner,
  data.name AS name,
  CASE
      -- Simple heuristic to differentiate between Redash and Lakeview dashboards
      WHEN CONTAINS(data.id, '-') THEN 'Redash'
      ELSE 'Lakeview'
  END AS dashboard_type,
  EXPLODE(failures) AS failure,
  -- Below are invisible column(s) used in links url templates
  CASE
    WHEN CONTAINS(data.id, '-') THEN CONCAT('/sql/dashboards/', data.id)
    ELSE CONCAT('/dashboardsv3/', data.id, '/published')
  END AS dashboard_link
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'Dashboard' AND SIZE(failures) > 0
ORDER BY workspace_id, owner, name, failure
