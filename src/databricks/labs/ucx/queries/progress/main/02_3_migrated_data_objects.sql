/* --title 'Migrated' --description 'Total number of tables and views' --height 6 */
SELECT COUNT(*) AS count
FROM ucx_catalog.multiworkspace.latest_historical_per_workspace
WHERE object_type = "migration_status" AND SIZE(failures) == 0
