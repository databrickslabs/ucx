/* --title 'Table migration readiness (%)' --width 2 */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM ucx_catalog.multiworkspace.historical
WHERE object_type = "tables"
