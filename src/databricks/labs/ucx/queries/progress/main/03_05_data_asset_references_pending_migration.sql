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
      WHEN object_type = 'UsedTable' THEN concat_ws('.', object_id)
      ELSE concat_ws('.', object_id)
  END AS object_id,
  explode(failures) AS failure,
  data.is_read,
  data.is_write,
  -- Below are invisible column(s) used in links url templates
  CASE
    WHEN object_type = 'DirectFsAccess' THEN concat('/#workspace/', data.source_id)
    WHEN object_type = 'UsedTable' THEN concat('/explore/data/', concat_ws('/', object_id))
    ELSE ''
  END AS link
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type IN ('DirectFsAccess', 'UsedTable')
