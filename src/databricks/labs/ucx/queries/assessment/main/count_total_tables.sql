-- viz type=counter, name=Total Table Count, counter_label=Total Tables, value_column=count_total_tables
-- widget col=1, row=0, size_x=1, size_y=3
SELECT count(*) AS count_total_tables
FROM $inventory.tables
