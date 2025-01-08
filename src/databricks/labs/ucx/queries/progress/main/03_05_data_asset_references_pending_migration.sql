/*
--title 'Data asset references'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "workspace_id", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "workspace_id"},
        {"fieldName": "object_type", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "object_type"},
        {"fieldName": "object_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ link }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "object_id"},
        {"fieldName": "failure", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "failure"},
        {"fieldName": "is_read", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "is_read"},
        {"fieldName": "is_write", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "is_write"}
      ]},
    "invisibleColumns": [
      {"name": "link", "booleanValues": ["false", "true"], "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "link"}
    ]
  }}'
*/
SELECT
  workspace_id,
  owner,
  CASE
    WHEN object_type = 'DirectFsAccess' THEN 'Direct filesystem access'
    WHEN object_type = 'UsedTable' THEN 'Table or view reference'
    ELSE object_type
  END AS object_type,
  CASE
      WHEN object_type = 'DirectFsAccess' THEN data.path
      WHEN object_type = 'UsedTable' THEN CONCAT_WS('.', object_id)
      ELSE CONCAT_WS('.', object_id)
  END AS object_id,
  EXPLODE(failures) AS failure,
  CAST(data.is_read AS BOOLEAN) AS is_read,
  CAST(data.is_write AS BOOLEAN) AS is_write,
  -- Below are invisible column(s) used in links url templates
  CASE
    -- SQL queries do NOT point to the workspace, i.e. start with '/'
    WHEN object_type = 'DirectFsAccess' AND SUBSTRING(data.source_id, 0, 1) != '/' THEN CONCAT('/sql/editor/', data.source_id)
    ELSE CONCAT('/#workspace', data.source_id)
  END AS link
FROM ucx_catalog.multiworkspace.objects_snapshot
ORDER BY workspace_id, owner, object_type, object_id
WHERE object_type IN ('DirectFsAccess', 'UsedTable')
