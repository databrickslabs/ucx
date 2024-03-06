-- viz type=counter, name=Total dlt estimates, counter_label=Total dlt estimated hours, value_column=total_estimated_hours
-- widget row=0, col=3, size_x=1, size_y=3
SELECT sum(estimated_hours) AS total_estimated_hours
FROM $inventory.object_estimates where object_type = "pipelines"
