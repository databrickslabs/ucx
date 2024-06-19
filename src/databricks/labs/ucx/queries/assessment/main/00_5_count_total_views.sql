-- --title 'Total View Count'
SELECT count(*) AS count_total_views
FROM inventory.tables where object_type = 'VIEW'
