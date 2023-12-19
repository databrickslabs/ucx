-- viz type=table, name=Table Types, search_by=name, columns=name,type,format,storage,is_delta,location
-- widget title=Table Types, row=1, col=3, size_x=3, size_y=8
SELECT CONCAT(`database`, '.', name) AS name,
       object_type AS type,
       table_format AS format,
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
