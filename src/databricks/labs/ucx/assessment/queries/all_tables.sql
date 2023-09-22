-- viz type=table, name=Table Types, columns=database,name,type,table_format,table_view,storage,is_delta,location
-- widget title=Table Types, col=0, row=0, size_x=6, size_y=6
SELECT
  `database`,
  name,
  object_type AS type,
  upper(table_format) AS format,
  CASE WHEN object_type IN ("MANAGED", "EXTERNAL") THEN "TABLE" ELSE "VIEW" END AS table_view,
  CASE WHEN SUBSTRING(location,1,4)="dbfs" AND SUBSTRING(location,6,10)<>"/mnt" THEN "DBFS ROOT"
       WHEN SUBSTRING(location,1,4)="dbfs" AND SUBSTRING(location,6,10)="/mnt" THEN "DBFS MOUNT"
       ELSE "EXTERNAL" END AS storage,
  CASE WHEN format LIKE "delta" THEN "Yes" ELSE "No" END AS is_delta,
  location
FROM hive_metastore.ucx.tables
