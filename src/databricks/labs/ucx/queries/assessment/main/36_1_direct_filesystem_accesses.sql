/*
--title 'Direct filesystem access problems'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "path", "title": "path", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
        {"fieldName": "is_read", "title": "is_read", "type": "boolean", "displayAs": "boolean", "booleanValues": ["false", "true"]},
        {"fieldName": "is_write", "title": "is_write", "type": "boolean", "displayAs": "boolean", "booleanValues": ["false", "true"]},
        {"fieldName": "source", "title": "source", "type": "string", "displayAs": "link", "linkUrlTemplate": "/jobs/{{ workflow_id }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "booleanValues": ["false", "true"]},
        {"fieldName": "timestamp", "title": "last_modified", "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)", "booleanValues": ["false", "true"]},
        {"fieldName": "lineage", "title": "lineage", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
        {"fieldName": "lineage_data", "title": "lineage_data", "type": "complex", "displayAs": "json", "booleanValues": ["false", "true"]},
        {"fieldName": "assessment_start", "title": "assessment_start", "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)", "booleanValues": ["false", "true"]},
        {"fieldName": "assessment_end", "title": "assessment_end", "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)", "booleanValues": ["false", "true"]}
      ]},
    "invisibleColumns": [
    {"fieldName": "lineage_type", "title": "lineage_type", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
    {"fieldName": "lineage_id", "title": "lineage_id", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]}
    ]
  }}'
*/
SELECT
  path,
  is_read,
  is_write,
  source_id as source,
  source_timestamp as `timestamp`,
  concat(lineage.object_type, ': ', lineage.object_id) as lineage,
  lineage.object_type as lineage_type,
  lineage.object_id as lineage_id,
  lineage.other as lineage_data,
  assessment_start,
  assessment_end
from (SELECT
  path,
  is_read,
  is_write,
  source_id,
  source_timestamp,
  explode(source_lineage) as lineage,
  assessment_start_timestamp as assessment_start,
  assessment_end_timestamp as assessment_end
FROM inventory.directfs)
