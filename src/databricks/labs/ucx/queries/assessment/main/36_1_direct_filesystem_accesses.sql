/*
--title 'Direct filesystem access problems'
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "location", "title": "location", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
        {"fieldName": "is_read", "title": "is_read", "type": "boolean", "displayAs": "boolean", "booleanValues": ["false", "true"]},
        {"fieldName": "is_write", "title": "is_write", "type": "boolean", "displayAs": "boolean", "booleanValues": ["false", "true"]},
        {"fieldName": "source", "title": "source", "type": "string", "displayAs": "link", "linkUrlTemplate": "{{ source_link }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "booleanValues": ["false", "true"]},
        {"fieldName": "timestamp", "title": "last_modified", "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)", "booleanValues": ["false", "true"]},
        {"fieldName": "lineage", "title": "lineage", "type": "string", "displayAs": "link", "linkUrlTemplate": "{{ lineage_link }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "booleanValues": ["false", "true"]},
        {"fieldName": "lineage_data", "title": "lineage_data", "type": "complex", "displayAs": "json", "booleanValues": ["false", "true"]},
        {"fieldName": "assessment_start", "title": "assessment_start", "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)", "booleanValues": ["false", "true"]},
        {"fieldName": "assessment_end", "title": "assessment_end", "type": "datetime", "displayAs": "datetime", "dateTimeFormat": "ll LTS (z)", "booleanValues": ["false", "true"]}
      ]},
    "invisibleColumns": [
    {"fieldName": "source_link", "title": "source_link", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
    {"fieldName": "lineage_type", "title": "lineage_type", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
    {"fieldName": "lineage_id", "title": "lineage_id", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]},
    {"fieldName": "lineage_link", "title": "lineage_link", "type": "string", "displayAs": "string", "booleanValues": ["false", "true"]}
    ]
  }}'
*/
SELECT
  path as location,
  is_read,
  is_write,
  if( startswith(source_id, '/'), substring_index(source_id, '@databricks.com/', -1), split_part(source_id, '/', 2)) as source,
  if( startswith(source_id, '/'), concat('/#workspace/', source_id), concat('/sql/editor/', split_part(source_id, '/', 2))) as source_link,
  source_timestamp as `timestamp`,
  case
    when lineage.object_type = 'WORKFLOW' then concat('Workflow: ', lineage.other.name)
    when lineage.object_type = 'TASK' then concat('Task: ', split_part(lineage.object_id, '/', 2))
    when lineage.object_type = 'NOTEBOOK' then concat('Notebook: ', substring_index(lineage.object_id, '@databricks.com/', -1))
    when lineage.object_type = 'FILE' then concat('File: ', substring_index(lineage.object_id, '@databricks.com/', -1))
    when lineage.object_type = 'DASHBOARD' then concat('Dashboard: ', lineage.other.name)
    when lineage.object_type = 'QUERY' then concat('Query: ', lineage.other.name)
  end as lineage,
  lineage.object_type as lineage_type,
  lineage.object_id as lineage_id,
  case
    when lineage.object_type = 'WORKFLOW' then concat('/jobs/', lineage.object_id)
    when lineage.object_type = 'TASK' then concat('/jobs/', split_part(lineage.object_id, '/', 1), '/tasks/', split_part(lineage.object_id, '/', 2))
    when lineage.object_type = 'NOTEBOOK' then concat('/#workspace/', lineage.object_id)
    when lineage.object_type = 'FILE' then concat('/#workspace/', lineage.object_id)
    when lineage.object_type = 'DASHBOARD' then concat('/sql/dashboards/', lineage.object_id)
    when lineage.object_type = 'QUERY' then concat('/sql/editor/', split_part(lineage.object_id, '/', 2))
  end as lineage_link,
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
