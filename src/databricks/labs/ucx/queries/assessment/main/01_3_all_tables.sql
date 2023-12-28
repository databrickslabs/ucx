-- viz type=table, name=Table Types, search_by=name, columns=name,type,format,storage,is_delta,location
-- widget title=Table Types, row=1, col=3, size_x=3, size_y=8
SELECT CONCAT(tables.`database`, '.', tables.name) AS name,
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
       location,
       size_in_bytes
FROM $inventory.tables left outer join $inventory.table_size on
$inventory.tables.catalog = $inventory.table_size.catalog and
$inventory.tables.database = $inventory.table_size.database and
$inventory.tables.name = $inventory.table_size.name
