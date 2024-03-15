-- viz type=counter, name=Data migration complexity, counter_label=Data migration complexity, value_column=data_migration_complexity
-- widget row=3, col=5, size_x=1, size_y=8
SELECT
CASE WHEN total_estimated_hours < 30 THEN "S"
 WHEN total_estimated_hours BETWEEN 30 AND 100 THEN "M"
 WHEN total_estimated_hours BETWEEN 100 AND 300 THEN "L"
 WHEN total_estimated_hours > 300 THEN "XL"
 ELSE NULL
END as data_migration_complexity FROM
(SELECT sum(estimated_hours) AS total_estimated_hours
FROM $inventory.table_estimates)