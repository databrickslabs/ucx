-- viz type=table, name=Cluster estimates, columns=catalog,database,name,object_type,table_format,estimated_hours
-- widget title=Cluster estimates, row=3, col=2, size_x=4, size_y=8
SELECT * FROM $inventory.object_estimates where object_type = "clusters"