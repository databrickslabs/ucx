-- viz type=table, name=DLT estimates, columns=catalog,database,name,object_type,table_format,estimated_hours
-- widget title=DLT estimates, row=4, col=2, size_x=4, size_y=8
SELECT * FROM $inventory.object_estimates where object_type = "pipelines"