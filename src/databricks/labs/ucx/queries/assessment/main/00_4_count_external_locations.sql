-- viz type=counter, name=Storage Locations, counter_label=Storage Locations, value_column=count_total
-- widget row=1, col=5, size_x=1, size_y=3
SELECT count(*) AS count_total
FROM $inventory.external_locations
