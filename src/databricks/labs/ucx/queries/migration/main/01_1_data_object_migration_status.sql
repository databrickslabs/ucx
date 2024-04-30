-- viz type=table, name=Migration status, search_by=table_name,upgraded_status, columns=table_name,object_type,table_format,location,upgraded_status,upgraded_to_table_name,view_text,storage_properties,is_partitioned
-- widget title=Migration status, row=1, col=2, size_x=4, size_y=8
SELECT
  concat_ws('.', tables.`catalog`, tables.`database`, tables.name) AS table_name,
  tables.object_type,
  tables.table_format,
  tables.location,
  CASE
      WHEN migration_status.dst_table IS NOT NULL THEN 'Yes'
      ELSE 'No'
  END AS upgraded_status,
  concat_ws('.', migration_status.dst_catalog, migration_status.dst_schema, migration_status.dst_table) AS upgraded_to_table_name,
  tables.view_text,
  tables.storage_properties,
  tables.is_partitioned
FROM
    $inventory.tables AS tables
  LEFT JOIN
    $inventory.migration_status AS migration_status
  ON tables.`database` = migration_status.src_schema AND tables.name = migration_status.src_table