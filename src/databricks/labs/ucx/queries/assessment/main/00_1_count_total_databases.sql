-- viz type=counter, name=Total Database Count, counter_label=Total Databases, value_column=count_total_databases
-- widget row=1, col=2, size_x=1, size_y=3
SELECT COUNT(DISTINCT `database`) AS count_total_databases
FROM $inventory.tables
