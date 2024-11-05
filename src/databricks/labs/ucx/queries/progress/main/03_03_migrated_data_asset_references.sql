/* --title 'Migrated' --description 'Total number of table, view and dfsa references' --height 6 */
SELECT COUNT(*) AS count
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type IN ('DirectFsAccess', 'UsedTable') AND SIZE(failures) == 0
