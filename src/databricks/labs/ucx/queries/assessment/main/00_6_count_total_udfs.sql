-- viz type=counter, name=Total UDF Count, counter_label=Total UDFs, value_column=count_total_udfs
-- widget row=2, col=4, size_x=1, size_y=3
SELECT count(*) AS count_total_udfs
FROM $inventory.udfs
