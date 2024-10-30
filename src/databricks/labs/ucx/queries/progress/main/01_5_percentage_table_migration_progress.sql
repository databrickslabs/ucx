/* --title 'Table migration progress (%)' --width 2 */
SELECT
    ROUND(100 * TRY_DIVIDE(COUNT_IF(SIZE(failures) = 0), COUNT(*)), 2) AS percentage
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = "Table"
