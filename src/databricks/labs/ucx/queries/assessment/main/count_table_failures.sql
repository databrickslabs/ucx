-- viz type=counter, name=Total Table Scan Failure Count, counter_label=Metastore Crawl Failures, value_column=count_failures
-- widget col=2, row=0, size_x=1, size_y=3
SELECT COUNT(*) AS count_failures
FROM $inventory.table_failures
