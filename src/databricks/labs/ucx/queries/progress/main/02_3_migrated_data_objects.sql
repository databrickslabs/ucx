/* --title 'Migrated' --description 'Total number of tables and views' --height 6 */
SELECT COUNT(*) AS count
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'Table' AND SIZE(failures) == 0
