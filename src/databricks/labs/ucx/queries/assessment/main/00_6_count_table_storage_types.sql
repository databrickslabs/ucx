-- viz type=table, name=Table counts by storage, columns=Storage,count
-- widget title=Table counts by storage, row=1, col=4, size_x=2, size_y=4
SELECT storage, COUNT(*) count
FROM (
SELECT
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
       END AS storage
FROM $inventory.tables)
WHERE storage IN ('DBFS ROOT', 'DBFS MOUNT', 'EXTERNAL')
GROUP BY storage
ORDER BY storage;