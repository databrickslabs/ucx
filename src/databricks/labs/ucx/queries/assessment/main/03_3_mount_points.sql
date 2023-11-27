-- viz type=table, name=Mount Points, columns=name,source
-- widget title=Mount Points, row=3, col=3, size_x=3, size_y=8
SELECT name,
       source
FROM $inventory.mounts