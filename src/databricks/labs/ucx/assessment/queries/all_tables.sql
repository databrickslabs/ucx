-- viz type=table, name=Table Types, columns=database,name,type,format,table_view,storage,is_delta,location
-- widget title=Table Types, col=0, row=0, size_x=6, size_y=6
SELECT `database`,
       name,
       object_type AS type,
       UPPER(table_format) AS format,
       IF(object_type IN ("MANAGED", "EXTERNAL"), "TABLE", "VIEW") AS table_view,
       CASE
           WHEN STARTSWITH(location, "/dbfs/")
                AND NOT STARTSWITH(location, "/dbfs/mnt") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "/dbfs/")
                AND STARTSWITH(location, "/dbfs/mnt") THEN "DBFS MOUNT"
           ELSE "EXTERNAL"
       END AS storage,
       IF(format = "delta", "Yes", "No") AS is_delta,
       location
FROM $inventory.tables
