-- viz type=table, name=Incompatible submit runs detected, columns=object_type,object_id,finding
-- widget title=Incompatible submit runs, row=0, col=2, size_x=4, size_y=8
SELECT * FROM
(SELECT object_type, object_id, EXPLODE(from_json(failures, 'array<string>')) AS finding
FROM $inventory.objects) 
WHERE finding = "no data security mode specified" 
  AND object_type = "submit_runs"

