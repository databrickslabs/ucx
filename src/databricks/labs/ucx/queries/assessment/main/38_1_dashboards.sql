/*
--title 'Dashboards'
--width 6
--overrides '{"spec": {
    "encodings": {
      "columns": [
        {"fieldName": "dashboard_type", "title": "Type", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
        {"fieldName": "name", "title": "Name", "type": "string", "displayAs": "link", "linkUrlTemplate": "{{ dashboard_link }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "booleanValues": ["false", "true"]}
      ]
    },
    "invisibleColumns": [
      {"fieldName": "dashboard_link", "title": "dashboard_link", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]}
    ]
  }}'
*/
SELECT
  dashboard_type,
  name,
  dashboard_link
FROM (
  SELECT
    'Redash' AS dashboard_type,
    name,
    CONCAT('/sql/dashboards/', id) AS dashboard_link
  FROM inventory.redash_dashboards
  UNION ALL
  SELECT
    'Lakeview' AS dashboard_type,
    name,
    CONCAT('/dashboardsv3/', id, '/published') AS dashboard_link
  FROM inventory.lakeview_dashboards
)
