-- viz type=counter, name=Total Database Count, counter_label=Total Databases, value_column=count_total_databases
-- widget col=0, row=0, size_x=1, size_y=3
SELECT COUNT(DISTINCT `database`) AS count_total_databases
FROM hive_metastore.ucx.tables
