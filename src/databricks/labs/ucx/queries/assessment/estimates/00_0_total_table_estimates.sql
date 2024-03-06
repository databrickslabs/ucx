-- viz type=counter, name=Total table estimates, counter_label=Total table estimated hours, value_column=total_estimated_hours
-- widget row=0, col=0, size_x=1, size_y=4
SELECT sum(estimated_hours) AS total_estimated_hours
FROM $inventory.table_estimates
