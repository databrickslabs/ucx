-- viz type=table, name=Table Types, search_by=name, columns=name,type,format,storage,is_delta,location
-- widget title=Table Types, row=13, col=0, size_x=8, size_y=8
SELECT CONCAT(tables.`database`, '.', tables.name) AS name,
       object_type AS type,
       table_format AS format,
       CASE
           WHEN STARTSWITH(location, "dbfs:/mnt") THEN "DBFS MOUNT"
           WHEN STARTSWITH(location, "/dbfs/mnt") THEN "DBFS MOUNT"
           WHEN STARTSWITH(location, "dbfs:/databricks-datasets") THEN "Databricks Demo Dataset"
           WHEN STARTSWITH(location, "/dbfs/databricks-datasets") THEN "Databricks Demo Dataset"
           WHEN STARTSWITH(location, "dbfs:/") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "/dbfs/") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "wasb") THEN "UNSUPPORTED"
           WHEN STARTSWITH(location, "adl") THEN "UNSUPPORTED"
           ELSE "EXTERNAL"
       END AS storage,
       IF(table_format = "DELTA", "Yes", "No") AS is_delta,
       location,
       CASE
            WHEN size_in_bytes IS null THEN "Non DBFS Root"
            WHEN size_in_bytes > 10000000000000000 THEN "SIZE OUT OF RANGE"
            WHEN size_in_bytes < 100 THEN CONCAT(CAST(size_in_bytes AS string)," Bytes")
            WHEN size_in_bytes < 100000 THEN CONCAT(CAST(round(size_in_bytes/1024,2) AS string),"KB")
            WHEN size_in_bytes < 100000000 THEN CONCAT(CAST(round(size_in_bytes/1024/1024,2) AS string),"MB")
            WHEN size_in_bytes < 100000000000 THEN CONCAT(CAST(round(size_in_bytes/1024/1024/1024,2) AS string),"GB")
            ELSE CONCAT(CAST(round(size_in_bytes/1024/1024/1024/1024,2) AS string),"TB")
       END AS table_size
FROM $inventory.tables left outer join $inventory.table_size on
$inventory.tables.catalog = $inventory.table_size.catalog and
$inventory.tables.database = $inventory.table_size.database and
$inventory.tables.name = $inventory.table_size.name
