-- viz type=table, name=External Locations, columns=location
-- widget title=External Locations, col=3, row=11, size_x=3, size_y=5
SELECT
  location
FROM
  $inventory.external_locations
