/* --title '"Table references in code" progress (%)' --description 'Tables referring UC over Hive metastore' */
SELECT
    ROUND(100 * TRY_DIVIDE(COUNT_IF(SIZE(failures) = 0), COUNT(*)), 2) AS percentage
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = "UsedTable"
