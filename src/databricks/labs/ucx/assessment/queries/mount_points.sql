-- viz type=table, name=Mount Points, columns=name, source
-- widget title=Mount Points, col=3, row=16, size_x=3, size_y=5
SELECT
  name, source
FROM
  $inventory.mounts
