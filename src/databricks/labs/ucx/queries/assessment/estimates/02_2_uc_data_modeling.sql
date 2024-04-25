-- viz type=table, name=Tables to migrate, search_by=catalog,database,name,upgraded_status, columns=catalog,database,name,object_type,table_format,location,view_text,upgraded_to,storage_properties
-- widget title=Tables to migrate, row=2, col=2, size_x=3, size_y=8
SELECT
  concat_ws('.', tables.`catalog`, tables.`database`, tables.name) AS table_name,
  tables.object_type,
  tables.table_format,
  tables.location,
  CASE
      WHEN migration_status.dst_table IS NOT NULL THEN 'Migrated'
      ELSE 'Not migrated'
  END AS upgraded_status,
  concat_ws('.', migration_status.`dst_catalog`, migration_status.`dst_schema`, migration_status.dst_table) AS upgraded_to_table_name,
  migration_status.update_ts AS upgraded_timestamp,
  tables.view_text,
  tables.storage_properties,
  tables.is_partitioned
FROM
    $inventory.tables AS tables
  LEFT JOIN
    $inventory.migration_status AS migration_status
  ON tables.`database` = migration_status.src_schema AND tables.name = migration_status.src_table