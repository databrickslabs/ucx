/* --title 'Migrated' --description 'Total number of table and view references' --height 6 */
SELECT COUNT(*) AS count
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'UsedTable' AND  SIZE(failures) == 0
