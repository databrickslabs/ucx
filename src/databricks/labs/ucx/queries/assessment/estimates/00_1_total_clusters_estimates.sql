-- viz type=counter, name=Total clusters estimates, counter_label=Total clusters estimated hours, value_column=total_estimated_hours
-- widget row=0, col=1, size_x=1, size_y=4
SELECT sum(estimated_hours) AS total_estimated_hours
FROM $inventory.object_estimates where object_type = "clusters"
