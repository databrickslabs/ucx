-- viz type=table, name=Table Types, columns=database,name,type,format,table_view,storage,is_delta,location
-- widget title=Table Types, col=0, row=23, size_x=6, size_y=6
SELECT `database`,
       name,
       object_type AS type,
       UPPER(table_format) AS format,
       IF(object_type IN ("MANAGED", "EXTERNAL"), "TABLE", "VIEW") AS table_view,
       CASE
           WHEN STARTSWITH(location, "dbfs:/mnt") THEN "DBFS MOUNT"
           WHEN STARTSWITH(location, "/dbfs/mnt") THEN "DBFS MOUNT"
           WHEN STARTSWITH(location, "dbfs:/") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "/dbfs/") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "wasb") THEN "UNSUPPORTED"
           WHEN STARTSWITH(location, "adl") THEN "UNSUPPORTED"
           ELSE "EXTERNAL"
       END AS storage,
       IF(format = "DELTA", "Yes", "No") AS is_delta,
       location
FROM $inventory.tables
