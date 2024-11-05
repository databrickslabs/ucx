/* --title 'Pending migration' --description 'Total number of tables and views' --height 6 */
SELECT COUNT(*) AS count
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'Table' AND array_contains(failures, 'Pending migration')
