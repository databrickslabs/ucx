-- viz type=table, name=Incompatible submit runs detected, columns=object_type,object_id,failure
-- widget title=Incimpatible submit runs, row=0, col=2, size_x=3, size_y=8
SELECT * FROM
(SELECT object_type, object_id, EXPLODE(from_json(failures, 'array<string>')) AS failure
FROM $inventory.objects) WHERE failure = "no data security mode specified" AND object_type = "submit_runs"

