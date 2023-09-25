-- viz type=table, name=Mount Points, columns=name,source
-- widget title=Mount Points, col=3, row=17, size_x=3, size_y=8
SELECT name,
       source
FROM hive_metastore.ucx.mounts