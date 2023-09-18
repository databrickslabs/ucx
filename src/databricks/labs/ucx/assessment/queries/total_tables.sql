-- UCX: type: table
SELECT
  COUNT(*) AS count,
  SUM(IF(object_type == "MANAGED", 1, 0)) AS managed_tables,
  ROUND(100 * IF(object_type == "MANAGED", 1, 0) / COUT(*), 0) AS managed_pct,
  SUM(IF(object_type == "EXTERNAL", 1, 0)) AS external_tables,
  ROUND(100 * IF(object_type == "EXTERNAL", 1, 0) / COUT(*), 0) AS external_pct,
  SUM(IF(object_type == "VIEW", 1, 0)) AS external_tables,
  ROUND(100 * IF(object_type == "VIEW", 1, 0) / COUT(*), 0) AS view_pct
FROM $inventory.tables