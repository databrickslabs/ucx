/* --title 'Database Summary' --filter database --width 6 */
WITH table_stats AS (
  SELECT
    `database`,
    object_type,
    table_format AS `format`,
    `location`,
    IF(object_type IN ('MANAGED', 'EXTERNAL'), 1, 0) AS is_table,
    IF(object_type = 'VIEW', 1, 0) AS is_view,
    CASE
      WHEN STARTSWITH(location, 'dbfs:/') AND NOT STARTSWITH(location, 'dbfs:/mnt')
      THEN 1
      WHEN STARTSWITH(location, '/dbfs/') AND NOT STARTSWITH(location, '/dbfs/mnt')
      THEN 1
      ELSE 0
    END AS is_dbfs_root,
    CASE WHEN STARTSWITH(location, 'wasb') THEN 1 WHEN STARTSWITH(location, 'adl') THEN 1 ELSE 0 END AS is_unsupported,
    IF(table_format = 'DELTA', 1, 0) AS is_delta
  FROM inventory.tables
), database_stats AS (
  SELECT
    `database`,
    CASE
      WHEN SUM(is_table) = 0 AND SUM(is_view) > 0
      THEN 'View Migration Required'
      WHEN SUM(is_dbfs_root) / SUM(is_table) > 0.3
      THEN 'Asset Replication Required'
      WHEN SUM(is_delta) / SUM(is_table) < 0.7
      THEN 'Some Non Delta Assets'
      WHEN SUM(is_unsupported) / SUM(is_table) > 0.7
      THEN 'Storage Migration Required'
      ELSE 'In Place Sync'
    END AS upgrade,
    SUM(is_table) AS tables,
    SUM(is_view) AS views,
    SUM(is_unsupported) AS unsupported,
    SUM(is_dbfs_root) AS dbfs_root,
    SUM(is_delta) AS delta_tables
  FROM table_stats
  GROUP BY
    `database`
), grant_stats AS (
  SELECT
    `database`,
    COUNT(*) AS total_grants,
    COUNT(DISTINCT principal) AS granted_principals,
    SUM(IF(object_type = 'DATABASE', 1, 0)) AS database_grants,
    SUM(IF(object_type = 'TABLE', 1, 0)) AS table_grants,
    SUM(IF(principal_type = 'service-principal', 1, 0)) AS service_principal_grants,
    SUM(IF(principal_type = 'user', 1, 0)) AS user_grants,
    SUM(IF(principal_type = 'group', 1, 0)) AS group_grants
  FROM inventory.grant_detail
  GROUP BY
    `database`
)
SELECT
  database,
  upgrade,
  tables,
  views,
  dbfs_root,
  delta_tables,
  total_grants,
  granted_principals,
  database_grants,
  table_grants,
  service_principal_grants,
  user_grants,
  group_grants
FROM database_stats
FULL JOIN grant_stats
  USING (`database`)
ORDER BY
  tables DESC