/*
--title 'Workflow migration problems'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "workspace_id", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "workspace_id"},
        {"fieldName": "object_type", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "object_type"},
        {"fieldName": "object_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "/#workspace/{{ source_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "link"},
        {"fieldName": "failure", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "failure"},
        {"fieldName": "is_read", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "is_read"},
        {"fieldName": "is_write", "booleanValues": ["false", "true"], "type": "integer", "displayAs": "number", "title": "is_write"}
      ]},
    "invisibleColumns": [
      {"name": "source_id", "booleanValues": ["false", "true"], "linkUrlTemplate": "/#workspace/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "source_id"}
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
  data.source_id
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type IN ('DirectFsAccess', 'UsedTable')
