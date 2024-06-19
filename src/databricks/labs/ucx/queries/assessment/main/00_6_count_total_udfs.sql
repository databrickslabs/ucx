-- --title 'Total UDF Count'
-- widget row=2, col=4, size_x=1, size_y=3
SELECT count(*) AS count_total_udfs
FROM inventory.udfs
