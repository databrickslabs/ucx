-- viz type=table, name=Database Summary, columns=database,tables,views,dbfs_root,delta_tables,upgraded_status,upgrade
-- widget title=Database Summary, col=0, row=9, size_x=6, size_y=8
SELECT `database`,
       SUM(is_table) AS tables,
       SUM(is_view) AS views,
       SUM(is_dbfs_root) AS dbfs_root,
       SUM(is_delta) AS delta_tables,
       SUM(upgrade_status) AS upgraded_tables
       CASE
           WHEN (SUM(upgrade_status) = SUM(is_table)) THEN "Fully Upgraded"
           WHEN (SUM(is_dbfs_root)/SUM(is_table) > .3) THEN "Asset Replication Required"
           WHEN (SUM(is_delta)/SUM(is_table) < .7) THEN "Some Non Delta Assets"
           ELSE "In Place Sync"
       END AS upgrade
FROM
  (SELECT DATABASE,
          name,
          object_type,
          UPPER(table_format) AS format,
          LOCATION,
          IF(object_type IN ("MANAGED", "EXTERNAL"), 1, 0) AS is_table,
          IF(object_type = "VIEW", 1, 0) AS is_view,
          IF(STARTSWITH(location, "/dbfs/") AND NOT STARTSWITH(location, "/dbfs/mnt"), 1, 0) AS is_dbfs_root,
          IF(UPPER(format) = "DELTA", 1, 0) AS is_delta,
          upgrade_status
   FROM $inventory.tables)
GROUP BY `database`
ORDER BY `database`