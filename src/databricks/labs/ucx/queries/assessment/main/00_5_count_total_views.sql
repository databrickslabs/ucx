-- viz type=counter, name=Total View Count, counter_label=Total Views, value_column=count_total_views
-- widget row=0, col=5, size_x=1, size_y=3
SELECT count(*) AS count_total_views FROM $inventory.tables where object_type = 'VIEW'