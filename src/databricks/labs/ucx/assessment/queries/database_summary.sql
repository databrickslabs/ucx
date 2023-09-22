-- viz type=table, name=Database Summary, columns=database,name,type,table_format,table_view,storage,is_delta,location
-- widget title=Database Summary, col=0, row=11, size_x=3, size_y=10
SELECT
  `database`,
  SUM(IS_TABLE) AS Tables,
  SUM(IS_VIEW) AS Views,
  SUM(IS_DBFS_Root) AS `DBFS Root`,
  SUM(IS_DELTA) AS `Delta Tables`,
  CASE
    WHEN (SUM(IS_DBFS_Root)/SUM(IS_TABLE) > .3) THEN "Full Asset Replication"
    WHEN (SUM(IS_DELTA)/SUM(IS_TABLE) < .7) THEN "Some Asset Replication"
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
  CASE WHEN object_type IN ("MANAGED","EXTERNAL") THEN 1 ELSE 0 END AS IS_TABLE,
  CASE WHEN object_type="VIEW" THEN 1 ELSE 0 END AS IS_VIEW,
  CASE WHEN SUBSTRING(location,1,4)="dbfs" AND SUBSTRING(location,6,10)<>"/mnt" THEN 1 ELSE 0 END AS IS_DBFS_Root,
  CASE WHEN format LIKE "delta" THEN 1 ELSE 0 END AS IS_DELTA
FROM $inventory.tables
)
GROUP BY `database`
ORDER BY `database`
