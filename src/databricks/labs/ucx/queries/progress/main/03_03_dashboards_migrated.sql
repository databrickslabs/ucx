/* --title 'Dashboards migrated' --height 6 */
SELECT COUNT(*) AS count
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'Dashboard' AND SIZE(failures) == 0
