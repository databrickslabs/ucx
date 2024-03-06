-- viz type=table, name=Jobs estimates, columns=catalog,database,name,object_type,table_format,estimated_hours
-- widget title=Jobs estimates, row=2, col=2, size_x=4, size_y=8
SELECT * FROM $inventory.object_estimates where object_type in ("jobs", "submit_runs")