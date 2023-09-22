-- viz type=table, name=Database Summary, columns=database,name,type,table_format,table_view,storage,is_delta,location
-- widget title=Database Summary, col=0, row=6, size_x=3, size_y=8
SELECT
  `database`,
  SUM(IS_TABLE) AS Tables,
  SUM(IS_VIEW) AS Views,
  SUM(IS_DBFS_Root) AS `DBFS Root`,
  SUM(IS_DELTA) AS `Delta Tables`,
  CASE
    WHEN (SUM(IS_DBFS_Root)/SUM(IS_TABLE) > .3) THEN "Asset Replication Required"
    WHEN (SUM(IS_DELTA)/SUM(IS_TABLE) < .7) THEN "Some Non Delta Assets"
    ELSE "In Place Sync"
  END AS Upgrade
FROM
(
SELECT
  database,
  name,
  object_type,
  UPPER(table_format) AS format,
  location,
  CASE WHEN object_type IN ("MANAGED","EXTERNAL") THEN 1 ELSE 0 END AS is_table,
  CASE WHEN object_type="VIEW" THEN 1 ELSE 0 END AS is_view,
  CASE WHEN SUBSTRING(location,0,3)="dbfs" AND SUBSTRING(location,4,9)<>"/mnt" THEN 1 ELSE 0 END AS is_dbfs_root,
  CASE WHEN upper(format) LIKE "DELTA" THEN 1 ELSE 0 END AS is_delta
FROM $inventory.tables
)
GROUP BY `database`
ORDER BY `database`
