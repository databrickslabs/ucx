/* --title 'Overall readiness (%)' --width 2 --height 6 */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM multiworkspace.historical
WHERE object_type != 'migration_status'
