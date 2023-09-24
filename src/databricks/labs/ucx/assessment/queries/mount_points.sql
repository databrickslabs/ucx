-- viz type=table, name=Mount Points, columns=name,source
-- widget title=Mount Points, col=3, row=10, size_x=3, size_y=4
SELECT name,
       source
FROM hive_metastore.ucx.mounts