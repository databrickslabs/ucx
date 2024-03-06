-- viz type=counter, name=Total jobs estimates, counter_label=Total jobs estimated hours, value_column=total_estimated_hours
-- widget row=0, col=2, size_x=1, size_y=4
SELECT sum(estimated_hours) AS total_estimated_hours
FROM $inventory.object_estimates where object_type in ("jobs", "submit_runs")
